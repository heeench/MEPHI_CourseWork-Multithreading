import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

public class CustomThreadPool {
    private final int corePoolSize;
    private final int maxPoolSize;
    private final long keepAliveTime;
    private final TimeUnit timeUnit;
    private final int queueSize;
    private final int minSpareThreads;

    private static final Logger logger = Logger.getLogger(CustomThreadPool.class.getName());

    private final List<Worker> workers;
    private final List<BlockingQueue<Runnable>> blockingQueues;
    private final CustomThreadFactory threadFactory;
    private final RejectedExecutionHandler rejectionHandler;

    private volatile boolean isShutdown = false;
    private final ReentrantLock mainLock = new ReentrantLock();
    private final AtomicInteger currentPoolSize = new AtomicInteger(0);
    private final AtomicInteger activeThreads = new AtomicInteger(0);
    private final AtomicInteger nextQueueIndex = new AtomicInteger(0);

    public CustomThreadPool(int corePoolSize, int maxPoolSize, long keepAliveTime,
                            TimeUnit timeUnit, int queueSize, int minSpareThreads) {
        if (corePoolSize < 0 || maxPoolSize <= 0 || maxPoolSize < corePoolSize ||
                keepAliveTime < 0 || queueSize <= 0 || minSpareThreads < 0) {
            throw new IllegalArgumentException("Invalid thread pool parameters");
        }

        this.corePoolSize = corePoolSize;
        this.maxPoolSize = maxPoolSize;
        this.keepAliveTime = keepAliveTime;
        this.timeUnit = timeUnit;
        this.queueSize = queueSize;
        this.minSpareThreads = minSpareThreads;

        this.workers = new ArrayList<>(maxPoolSize);
        this.blockingQueues = new ArrayList<>(maxPoolSize);
        this.threadFactory = new CustomThreadFactory();
        this.rejectionHandler = new CustomRejectionHandler();

        initPool();
    }

    private void initPool() {
        for (int i = 0; i < corePoolSize; i++) {
            createWorker();
        }

        logger.info("Thread pool initialized with " + corePoolSize + " core threads");
    }

    private void createWorker() {
        BlockingQueue<Runnable> queue = new LinkedBlockingQueue<>();
        blockingQueues.add(queue);
        Worker worker = new Worker(queue);
        workers.add(worker);
        Thread thread = threadFactory.newThread(worker);
        thread.start();
        currentPoolSize.incrementAndGet();
    }

    public void execute(Runnable task) {
        if (task == null) {
            throw new NullPointerException("Task cannot be null");
        }

        if (isShutdown) {
            rejectionHandler.rejectedExecution(task,null);
            return;
        }

        mainLock.lock();

        try {
            int activeCount = activeThreads.get();
            int currentSize = currentPoolSize.get();

            if (activeCount >= currentSize && currentSize < maxPoolSize) {
                createWorker();
                logger.info("Created new worker thread. Current pool size: " + currentSize);
            }

            BlockingQueue<Runnable> targetQueue = getTargetQueue();

            if (!targetQueue.offer(task)) {
                rejectionHandler.rejectedExecution(task, null);
            } else {
                logger.fine("Task submitted to queue " + blockingQueues.indexOf(targetQueue));
            }
        } finally {
            mainLock.lock();
        }
    }

    public void shutdown() {
        mainLock.lock();
        try {
            isShutdown = true;
            for (Worker worker : workers) {
                worker.interrupt();
            }
            logger.info("Thread pool shutdown initiated");
        } finally {
            mainLock.unlock();
        }
    }

    private BlockingQueue<Runnable> getTargetQueue() {
        int index = nextQueueIndex.getAndIncrement() % blockingQueues.size();
        return blockingQueues.get(index);
    }

    private class Worker implements Runnable {
        private final BlockingQueue<Runnable> queue;
        private volatile boolean isRunning = true;

        public Worker(BlockingQueue<Runnable> queue) {
            this.queue = queue;
        }

        public void interrupt() {
            isRunning = false;
            Thread.currentThread().interrupt();
        }

        @Override
        public void run() {
            while (isRunning) {
                try {
                    Runnable task = queue.poll(keepAliveTime, timeUnit);
                    if (task != null) {
                        activeThreads.incrementAndGet();
                        try {
                            task.run();
                        } finally {
                            activeThreads.decrementAndGet();
                        }
                    } else if (currentPoolSize.get() > corePoolSize) {
                        break;
                    }
                } catch (InterruptedException ex) {
                    if (!isRunning) {
                        break;
                    }
                }
            }


            currentPoolSize.decrementAndGet();
            workers.remove(this);
            logger.info("Worker thread terminated. Current pool size: " + currentPoolSize.get());
        }
    }

    private static class CustomThreadFactory implements ThreadFactory {
        private final AtomicInteger threadNumber = new AtomicInteger(1);

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r, "CustomThreadPool-Worker-" + threadNumber.getAndIncrement());
            t.setDaemon(false);
            logger.info("Created new thread: " + t.getName());
            return t;
        }
    }

    private static class CustomRejectionHandler implements RejectedExecutionHandler {
        @Override
        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
            logger.warning("Task rejected: " + r.toString());
        }
    }
}
