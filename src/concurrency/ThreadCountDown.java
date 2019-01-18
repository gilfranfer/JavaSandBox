package concurrency;

import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/** Testing CountDownLatch and ExecutorService to manage scenario where
 * multiple Threads work together to complete tasks from a single
 * resource provider, so the processing can be faster. */
public class ThreadCountDown {

    private CountDownLatch threadsCountdown = null;
    private static Queue<Integer> tasks = new PriorityQueue<>();

    public static void main(String[] args) {
        // Create a queue with "Tasks"
        int numberOfTasks = 100;
        while(numberOfTasks-- > 0) {
            tasks.add(numberOfTasks);
        }

        // Initiate Processing of Tasks
        ThreadCountDown main = new ThreadCountDown();
        main.process(tasks);
    }

    /* Receiving the Tasks to process, and creating multiple Threads
    * to process in parallel. */
    private void process(Queue<Integer> tasks) {
        int numberOfThreads = getNumberOfThreadsRequired(tasks.size());
        threadsCountdown = new CountDownLatch(numberOfThreads);
        ExecutorService threadExecutor = Executors.newFixedThreadPool(numberOfThreads);

        //Initialize each Thread
        while(numberOfThreads-- > 0) {
            System.out.println("Initializing Thread: "+numberOfThreads);
            threadExecutor.execute(new MyThread("Thread "+numberOfThreads));
        }

        try {
            //Shutdown the Executor, so it cannot receive more Threads.
            threadExecutor.shutdown();
            threadsCountdown.await();
            System.out.println("ALL THREADS COMPLETED!");
        } catch (InterruptedException ex) {
            ex.printStackTrace();
        }
    }

    /* Determine the number of Threads to create, based on a the max number
     * of tasks a Thread should work on */
    private int getNumberOfThreadsRequired(int size) {
        int maxTaskPerThread = 10;
        int threads = size / maxTaskPerThread;
        if( size > (threads*maxTaskPerThread) ){
            threads++;
        }
        return threads;
    }

    /* Task Provider. All Threads will get their task from here */
    private synchronized static Integer getTask(){
        return tasks.poll();
    }

    /* The Threads will get Tasks and process them, while still available.
    * When no more tasks available, the thread will complete and reduce the threadsCountdown */
    private class MyThread implements Runnable {

        private String threadName;

        protected MyThread(String threadName) {
            super();
            this.threadName = threadName;
        }

        @Override
        public void run() {
            Integer task;
            try{
                //Check in the Task pool if anything pending to process
                while( (task = getTask()) != null ){
                    processTask(task);
                }
            }catch (Exception ex){
                ex.printStackTrace();
            }finally {
                /*Reduce count when no more tasks to process. Eventually all
                Threads will end-up here, reducing the count to 0, allowing
                the flow to continue after threadsCountdown.await(); */
                threadsCountdown.countDown();
            }
        }

        private void processTask(Integer task){
            try{
                System.out.println(this.threadName+" is Working on Task: "+ task);
            }catch (Exception ex){
                ex.printStackTrace();
            }
        }
    }
}
