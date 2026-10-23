package com.netpulse.memory;

import sun.misc.Unsafe;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

public final class DirectBufferArena {

    private static final Unsafe UNSAFE;
    static {
        try {
            Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            UNSAFE = (Unsafe) field.get(null);
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize sun.misc.Unsafe", e);
        }
    }

    private static final int CACHE_LINE_SIZE = 64;
    private static final int MIN_BLOCK_SIZE = 64;
    private static final int MAX_ORDER = 11; // 2^11 * 64 = 128KB max block size
    private static final int NUM_ORDERS = MAX_ORDER + 1;

    private final long arenaAddress;
    private final long arenaSize;
    private final int totalBlocks;
    
    // Buddy allocation tracking
    private final byte[] blockStatus; // 0 = free, >0 = order allocated
    private final long[] freeLists;   // Pointers to heads of free lists per order
    
    private static final AtomicIntegerFieldUpdater<DirectBufferArena> ALLOCATED_BYTES_UPDATER =
        AtomicIntegerFieldUpdater.newUpdater(DirectBufferArena.class, "allocatedBytes");
    private volatile int allocatedBytes = 0;

    public DirectBufferArena(int capacityBytes) {
        // Align capacity to 128KB block boundary
        int maxBlockSize = MIN_BLOCK_SIZE * (1 << MAX_ORDER);
        if (capacityBytes % maxBlockSize != 0) {
            capacityBytes = ((capacityBytes + maxBlockSize - 1) / maxBlockSize) * maxBlockSize;
        }
        this.arenaSize = capacityBytes;
        this.totalBlocks = capacityBytes / MIN_BLOCK_SIZE;

        // Allocate off-heap memory aligned to cache line
        long rawAddress = UNSAFE.allocateMemory(arenaSize + CACHE_LINE_SIZE);
        long offset = rawAddress % CACHE_LINE_SIZE;
        this.arenaAddress = (offset == 0) ? rawAddress : rawAddress + (CACHE_LINE_SIZE - offset);

        this.blockStatus = new byte[totalBlocks];
        this.freeLists = new long[NUM_ORDERS];

        for (int i = 0; i < NUM_ORDERS; i++) {
            freeLists[i] = 0L; // Null pointers
        }

        // Initialize buddy system: divide arena into largest possible blocks
        initializeBuddyBlocks();
    }

    private void initializeBuddyBlocks() {
        int currentBlock = 0;
        int remainingBlocks = totalBlocks;

        while (remainingBlocks > 0) {
            int order = 31 - Integer.numberOfLeadingZeros(remainingBlocks);
            if (order > MAX_ORDER) {
                order = MAX_ORDER;
            }
            int blockSizeBlocks = 1 << order;

            long blockAddress = blockIndexToAddress(currentBlock);
            writeFreeNode(blockAddress, 0L); // No next pointer
            
            // Link into free list for this order
            freeLists[order] = blockAddress;

            currentBlock += blockSizeBlocks;
            remainingBlocks -= blockSizeBlocks;
        }
    }

    public synchronized ByteBuffer allocate(int size) {
        if (size <= 0) {
            throw new IllegalArgumentException("Allocation size must be positive");
        }

        int requiredSize = Math.max(size, MIN_BLOCK_SIZE);
        int targetOrder = 0;
        int currentSize = MIN_BLOCK_SIZE;

        while (currentSize < requiredSize) {
            currentSize <<= 1;
            targetOrder++;
        }

        if (targetOrder > MAX_ORDER) {
            throw new OutOfMemoryError("Requested allocation size exceeds maximum block size");
        }

        long blockAddress = allocateBlock(targetOrder);
        if (blockAddress == 0L) {
            throw new OutOfMemoryError("DirectBufferArena out of memory");
        }

        ALLOCATED_BYTES_UPDATER.addAndGet(currentSize);

        // Wrap off-heap memory into a direct ByteBuffer without JVM cleaning overhead
        try {
            ByteBuffer buffer = ByteBuffer.allocateDirect(0);
            Field addressField = ByteBuffer.class.getDeclaredField("address");
            addressField.setAccessible(true);
            addressField.set(buffer, blockAddress);

            Field capacityField = ByteBuffer.class.getDeclaredField("capacity");
            capacityField.setAccessible(true);
            capacityField.set(buffer, currentSize);

            buffer.clear();
            return buffer;
        } catch (Exception e) {
            freeBlock(blockAddress, targetOrder);
            ALLOCATED_BYTES_UPDATER.addAndGet(-currentSize);
            throw new RuntimeException("Failed to construct direct ByteBuffer wrapper", e);
        }
    }

    private long allocateBlock(int order) {
        if (order > MAX_ORDER) {
            return 0L;
        }

        if (freeLists[order] != 0L) {
            long blockAddress = freeLists[order];
            freeLists[order] = readFreeNode(blockAddress);

            int blockIdx = addressToBlockIndex(blockAddress);
            blockStatus[blockIdx] = (byte) (order + 1);
            return blockAddress;
        }

        // Try splitting a larger block
        long higherBlockAddress = allocateBlock(order + 1);
        if (higherBlockAddress == 0L) {
            return 0L;
        }

        int blockSize = MIN_BLOCK_SIZE * (1 << order);
        long buddyAddress = higherBlockAddress + blockSize;

        // Link buddy into current order's free list
        writeFreeNode(buddyAddress, freeLists[order]);
        freeLists[order] = buddyAddress;

        int buddyIdx = addressToBlockIndex(buddyAddress);
        blockStatus[buddyIdx] = 0; // Free

        int higherIdx = addressToBlockIndex(higherBlockAddress);
        blockStatus[higherIdx] = (byte) (order + 1);

        return higherBlockAddress;
    }

    public synchronized void free(ByteBuffer buffer) {
        if (buffer == null || !buffer.isDirect()) {
            return;
        }

        long address;
        try {
            Field addressField = ByteBuffer.class.getDeclaredField("address");
            addressField.setAccessible(true);
            address = addressField.getLong(buffer);
        } catch (Exception e) {
            throw new RuntimeException("Failed to extract address from ByteBuffer", e);
        }

        int blockIdx = addressToBlockIndex(address);
        if (blockIdx < 0 || blockIdx >= totalBlocks) {
            throw new IllegalArgumentException("Address out of arena bounds");
        }

        byte status = blockStatus[blockIdx];
        if (status == 0) {
            throw new IllegalStateException("Double free or unallocated block detected");
        }

        int order = status - 1;
        blockStatus[blockIdx] = 0;

        freeBlock(address, order);

        int freedSize = MIN_BLOCK_SIZE * (1 << order);
        ALLOCATED_BYTES_UPDATER.addAndGet(-freedSize);
    }

    private void freeBlock(long address, int order) {
        while (order < MAX_ORDER) {
            int blockIdx = addressToBlockIndex(address);
            int blockSize = MIN_BLOCK_SIZE * (1 << order);
            long buddyAddress = (blockIdx % (1 << (order + 1)) == 0) ? 
                address + blockSize : address - blockSize;

            if (buddyAddress < arenaAddress || buddyAddress >= arenaAddress + arenaSize) {
                break;
            }

            int buddyIdx = addressToBlockIndex(buddyAddress);
            if (blockStatus[buddyIdx] != 0 || (blockStatus[buddyIdx] - 1) != order) {
                break; // Buddy is allocated or of different order
            }

            // Remove buddy from free list
            removeFreeNode(buddyAddress, order);
            blockStatus[buddyIdx] = 0;

            // Merge with buddy
            if (buddyAddress < address) {
                address = buddyAddress;
            }
            order++;
        }

        int finalIdx = addressToBlockIndex(address);
        blockStatus[finalIdx] = 0;
        writeFreeNode(address, freeLists[order]);
        freeLists[order] = address;
    }

    private void removeFreeNode(long targetAddress, int order) {
        long current = freeLists[order];
        if (current == targetAddress) {
            freeLists[order] = readFreeNode(current);
            return;
        }

        while (current != 0L) {
            long next = readFreeNode(current);
            if (next == targetAddress) {
                writeFreeNode(current, readFreeNode(next));
                return;
            }
            current = next;
        }
    }

    private long blockIndexToAddress(int blockIndex) {
        return arenaAddress + ((long) blockIndex * MIN_BLOCK_SIZE);
    }

    private int addressToBlockIndex(long address) {
        return (int) ((address - arenaAddress) / MIN_BLOCK_SIZE);
    }

    private long readFreeNode(long address) {
        return UNSAFE.getLong(address);
    }

    private void writeFreeNode(long address, long nextAddress) {
        UNSAFE.putLong(address, nextAddress);
    }

    public int getAllocatedBytes() {
        return allocatedBytes;
    }

    public int getCapacity() {
        return (int) arenaSize;
    }

    public synchronized void release() {
        if (arenaAddress != 0L) {
            UNSAFE.freeMemory(arenaAddress - (arenaAddress % CACHE_LINE_SIZE));
        }
    }
}