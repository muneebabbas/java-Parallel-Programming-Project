import java.util.Random;
import java.util.concurrent.Callable;
import java.time.Instant;
import java.time.Duration;
import java.util.concurrent.ForkJoinTask;

public class Test {
	public static final int x = 10000;
	public static final int y = 1000;
  	public static final int SEQUENTIAL_CUTOFF_ROWS = 20;
    public static final int SEQUENTIAL_CUTOFF_COLS  = 100000;

	public static void main(String[] args) throws Exception {
		Object[][] arr = new Object[x][y];
		timedRun(()-> {
			for (int i = 0; i<x; ++i)
				for (int j = 0; j<y; ++j)
					arr[i][j] = new Object();
			return "Sequential Init: ";
		});
		timedRun(()->{
			initLocks(arr, 0, x, 0, y);
			return "Parallel Init: ";
		});
	}

	static void initLocks(Object[][] locks, int rowBegin, int rowEnd, int colBegin, int colEnd) {
        if (rowEnd - rowBegin <= SEQUENTIAL_CUTOFF_ROWS) 
        {
            if (colEnd - colBegin <= SEQUENTIAL_CUTOFF_COLS) 
                for (int i = rowBegin; i < rowEnd; ++i)
                    for (int j = colBegin; j < colEnd; ++j)
                        locks[i][j] = new Object();
            else {
                ForkJoinTask<?> left = ForkJoinTask.adapt(()-> initLocks(locks, rowBegin, 
                    rowEnd, colBegin, (colBegin+colEnd)/2)).fork();
                initLocks(locks, rowBegin, rowEnd, (colBegin+colEnd)/2, colEnd);
                left.join();
            }
        } 
        else 
        {
            ForkJoinTask<?> left = ForkJoinTask.adapt(()-> initLocks(locks, rowBegin, 
                (rowBegin+rowEnd)/2, colBegin, colEnd)).fork();
            initLocks(locks, (rowBegin+rowEnd)/2, rowEnd, colBegin, colEnd);
            left.join();
        }
    }

    static <T> void timedRun(Callable<T> function) throws Exception {
        Instant start = Instant.now();
        System.out.println(function.call() + ""
             + Duration.between(start, Instant.now()).toMillis());
    }

}