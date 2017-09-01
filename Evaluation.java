import java.util.Random;
import java.util.concurrent.Callable;
import java.time.Instant;
import java.time.Duration;

public class Evaluation {
	public static final int NUM_QUERIES = 500;
	public static final int TIMES = 100; // number of times to run and take average
	public static int x = 100;
	public static int y = 50;

	public static void main(String[] args) throws Exception {
		CensusData res = PopulationQuery.parse("CenPop2010.txt");		
		int[][] queries = generateQueries(x, y, NUM_QUERIES);
		int[] result = new int[NUM_QUERIES];
		PopulationQuery.preprocessOne(res, x, y);
		for (int i = 0; i<NUM_QUERIES; ++i) {
			System.out.printf("%d %d %d %d ", queries[i][0], queries[i][1], queries[i][2], queries[i][3]);		
			result[i] = PopulationQuery.versionOne(res, x, y, queries[i]).getElementA();
			System.out.println(result[i]);
		}
	}



	public static boolean checkCorrectness() throws Exception {
		CensusData res = PopulationQuery.parse("CenPop2010.txt");
		int[][] queries = generateQueries(x, y, NUM_QUERIES);
		int[] result = new int[NUM_QUERIES];
		PopulationQuery.preprocessOne(res, x, y);
		for (int i = 0; i<NUM_QUERIES; ++i) {
			result[i] = PopulationQuery.versionOne(res, x, y, queries[i]).getElementA();
		}

		int[] result2 = new int[NUM_QUERIES];
		PopulationQuery.preprocessTwo(res, x, y);
		for (int i = 0; i<NUM_QUERIES; ++i) {
			result2[i] = PopulationQuery.versionTwo(res, x, y, queries[i]).getElementA();
		}

		if (compare(result, result2, NUM_QUERIES) != true)
			return false;

		PopulationQuery.preprocessThree(res, x, y);
		for (int i = 0; i<NUM_QUERIES; ++i) {
			result2[i] = PopulationQuery.versionThree(x, y, queries[i]).getElementA();
		}
		if (compare(result, result2, NUM_QUERIES) != true)
			return false;

		PopulationQuery.preprocessFour(res, x, y);
		for (int i = 0; i<NUM_QUERIES; ++i) {
			result2[i] = PopulationQuery.versionThree(x, y, queries[i]).getElementA();
		}

		if (compare(result, result2, NUM_QUERIES) != true)
			return false;

		PopulationQuery.preprocessFive(res, x, y);
		for (int i = 0; i<NUM_QUERIES; ++i) {
			result2[i] = PopulationQuery.versionThree(x, y, queries[i]).getElementA();
		}
		if (compare(result, result2, NUM_QUERIES) != true)
			return false;

		return true;
		
	}

	public static boolean compare(int[] a, int[] b, int len) {
		for (int i = 0; i<len; ++i) {
			if (a[i] != b[i])
				return false;
		}
		return true;
	}

	public static void part8() throws Exception {
		CensusData res = PopulationQuery.parse("CenPop2010.txt");
		int[][] queries = generateQueries(x, y, NUM_QUERIES);
		PopulationQuery.preprocessTwo(res, x, y);
		System.out.println("Version1");
		for(int i = 1; i < NUM_QUERIES; i+=10) {
			final int num_queries = i;
			timedRun(()-> {
				for (int j = 0; j < num_queries; ++j) {
					PopulationQuery.versionTwo(res, x, y, queries[j]);
				}
				return num_queries + " ";
			});
		}
		timedRun(()->{
			for (int t = 0; t < 50; ++t)
				PopulationQuery.preprocessFour(res, x, y);
			return "PreprocessThree ";
		});
		System.out.println("Version2");
		for(int i = 1; i < NUM_QUERIES; i+= 50) {
			final int num_queries = i;
			timedRun(()-> {
				for (int j = 0; j < num_queries; ++j) {
					PopulationQuery.versionThree(x, y, queries[j]);
				}
				return num_queries + " ";
			});
		}

	}


	public static void part7() throws Exception {
		CensusData res = PopulationQuery.parse("CenPop2010.txt");
		System.out.println("Preprocess Four");
		for(int i = 10; i < 1000; i+= 10) {
			final int grid = i;
			timedRun(()-> {
				for(int j = 0; j < 50; ++j) {
					PopulationQuery.preprocessFour(res, grid, grid);
				}
				return grid + " ";
			});
		}
		System.out.println("Preprocess Five");
		for(int i = 10; i < 1000; i+= 10) {
			final int grid = i;
			timedRun(()-> {
				for(int j = 0; j < 50; ++j) {
					PopulationQuery.preprocessFive(res, grid, grid);
				}
				return grid + " ";
			});
		}
		// changing size of grid
	}

	public static void part6() throws Exception {
		int[][] queries = generateQueries(x, y, NUM_QUERIES);
		CensusData res = PopulationQuery.parse("CenPop2010.txt");
		System.out.println("Version 1");
		PopulationQuery.preprocessOne(res, x, y); // warming up the jvm
		Instant start = Instant.now();
		for (int i = 0; i < 100; ++i) {
			PopulationQuery.preprocessOne(res, x, y);
		} 

		System.out.println("preprocessOne " + Duration.between(start, Instant.now()).toMillis() + "ms");

		timedRun(() -> {
			for(int i = 0; i < NUM_QUERIES; ++i) {
				PopulationQuery.versionOne(res, x, y, queries[i]);
			}

			return "Queries ";
		});

		PopulationQuery.preprocessTwo(res, x, y); // warm up fork join
		System.out.println("\nVersion 2");
		start = Instant.now();
		for (int i = 0; i < 100; ++i) {
			PopulationQuery.preprocessTwo(res, x, y);
		} 

		System.out.println("preprocessTwo " + Duration.between(start, Instant.now()).toMillis() + "ms");
		PopulationQuery.versionTwo(res, x, y, queries[0]); // warming up
		for (int j = 1000; j < 250000; j+=10000) {
			PopulationQuery.SEQUENTIAL_CUTOFF = j;
			timedRun(() -> {
				for(int i = 0; i < 100; ++i) {
					// PopulationQuery.versionTwo(res, x, y, queries[i]);
					PopulationQuery.preprocessTwo(res, x, y);
				}
				return PopulationQuery.SEQUENTIAL_CUTOFF + " ";
			});
		}
	}

//=====================================================================
// Generate n random valid queries
//=====================================================================

	public static int[][] generateQueries(int x, int y, int num) {
		int [][] queries = new int[num][4];
		Random rand = new Random();
		for (int i = 0; i < num; ++i) {
			int w = rand.nextInt(x) + 1;
			int s = rand.nextInt(y) + 1;
			int e = rand.nextInt(x-w+1) + w;
			int n = rand.nextInt(y-s+1) + s;
			int[] query = {w, s, e, n};
			if (PopulationQuery.isCorrectQuery(query, x, y)) {
				// System.out.printf("%d %d %d %d\n", w, s, e, n);
			} else {
				System.out.printf("WrongQG -> %d %d %d %d\n", w, s, e, n);
				System.exit(-1);
			}
			queries[i] = query;
		}
		return queries;
	}

    static <T> void timedRun(Callable<T> function) throws Exception {
        Instant start = Instant.now();
        System.out.println(function.call() + ""
             + Duration.between(start, Instant.now()).toMillis());
    }

}