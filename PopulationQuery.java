import static java.lang.Math.*;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.stream.*;
import java.util.Arrays;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class PopulationQuery {
	// next four constants are relevant to parsing
	public static final int TOKENS_PER_LINE  = 7;
	public static final int POPULATION_INDEX = 4; // zero-based indices
	public static final int LATITUDE_INDEX   = 5;
	public static final int LONGITUDE_INDEX  = 6;
    public static final BufferedReader BR = 
                new BufferedReader(new InputStreamReader(System.in));
    public static int SEQUENTIAL_CUTOFF = 25000;
    public static final int SEQUENTIAL_CUTOFF_ROWS = 20;
    public static final int SEQUENTIAL_CUTOFF_COLS  = 100000;
    public static final int THREADS = 4;
    // Variables that are set in the preprocess stage
    public static Corners CORNERS = null;
    public static int[][] GRID = null;
    public static int POPULATION = 0;
    public static CensusData result = null;
    public static int X = 0;
    public static int Y = 0;
    public static int VERSION = -1;
	// parse the input file into a large array held in a CensusData object
	public static CensusData parse(String filename) {
		CensusData result = new CensusData();
		
        try {
            BufferedReader fileIn = new BufferedReader(new FileReader(filename));
            String oneLine = fileIn.readLine(); // skip the first line

            while ((oneLine = fileIn.readLine()) != null) {
                String[] tokens = oneLine.split(",");
                if(tokens.length != TOKENS_PER_LINE)
                	throw new NumberFormatException();
                int population = Integer.parseInt(tokens[POPULATION_INDEX]);
                if(population != 0) {
                    float latitude = Float.parseFloat(tokens[LATITUDE_INDEX]);
                	result.add(population, latitude,
                		       Float.parseFloat(tokens[LONGITUDE_INDEX]));
                }
            }
            fileIn.close();
        } catch(IOException ioe) {
            System.err.println("Error opening/reading/writing input or output file.");
            System.exit(1);
        } catch(NumberFormatException nfe) {
            System.err.println(nfe.toString());
            System.err.println("Error in file format");
            System.exit(1);
        }
        return result;
	}
	// argument 1: file name for input data: pass this to parse
	// argument 2: number of x-dimension buckets
	// argument 3: number of y-dimension buckets
    // argument 4: -v1, -v2, -v3, -v4, or -v5
	public static void main(String[] args) {
        if (args.length < 4 || args.length > 4) {
            System.out.println("Exiting Program ...");
            System.exit(1);
        }
        X = Integer.parseInt(args[1]);
        Y = Integer.parseInt(args[2]);
        String version = args[3];
        String filename = args[0];
        CensusData result = parse(filename);
        switch(version) {
            case "-v1" : preprocessOne(result, X, Y);     break;
            case "-v2" : preprocessTwo(result, X, Y);     break;
            case "-v3" : preprocessThree(result, X, Y);   break;
            case "-v4" : preprocessFour(result, X, Y);    break;
            case "-v5" : preprocessFive(result, X, Y);    break;
            default: System.out.println("Illegal version given. Exiting Program");
            System.exit(-1);
        }
        Pair<Integer, Float> ans = null;
        while(true) {
            int[] query = getQuery();
            if (version.equals("-v1")) {
                ans = versionOne(result, X, Y, query);
            }
            else if (version.equals("-v2")) {
                ans = versionTwo(result, X, Y, query);
            }
            else {
                ans = versionThree(X, Y, query);
            }
            System.out.printf("population: %d  percentage: %.2f\n", ans.getElementA(), ans.getElementB());
        }
	}

//*******************************************************************************************************
// Part 1
// ======
// Before processing any queries, process the data to find the four corners of the U.S. rectangle
// using a sequential O(n) algorithm where n is the number of census-block-groups. Then for each 
// query do another sequential O(n) traversal to answer the query (determining for each census-block-group 
// whether or not it is in the query rectangle). The simplest and most reusable approach for each 
// census-block-group is probably to first compute what grid position it is in and then see if this 
// grid position is in the query rectangle.
//*******************************************************************************************************
    public static void preprocessOne(CensusData res, int x, int y) {
        float maxLat = res.data[0].latitude;
        float minLat = maxLat;
        float maxLong = res.data[0].longitude;
        float minLong = maxLong;
        POPULATION = 0;
        for (int i=0; i<res.data_size; ++i) {
            maxLat = max(res.data[i].latitude, maxLat);
            minLat = min(res.data[i].latitude, minLat);
            maxLong = max(res.data[i].longitude, maxLong);
            minLong = min(res.data[i].longitude, minLong);
            POPULATION = POPULATION + res.data[i].population;
        }
        // System.out.printf("maxLat:%f minLat:%f\n", maxLat, minLat);
        // System.out.printf("maxLong:%f minLong:%f\n", maxLong, minLong);
        CORNERS = new Corners(maxLat, minLat, maxLong, minLong, POPULATION);
    }
    public static Pair<Integer, Float> versionOne(CensusData res, int x, int y, int[] query) {
        int popCount = 0;
        for (int i=0; i<res.data_size; ++i) {
            int[] censusPos = censusToGrid(res.data[i], x, y,
                 CORNERS.minLong, CORNERS.maxLong, CORNERS.minLat, CORNERS.maxLat);
            if (inGrid(censusPos, query)) {
                popCount = popCount + res.data[i].population;
            }
        }
        float percentage = (float) popCount/POPULATION * 100;
        return new Pair<Integer, Float>(popCount, percentage);
    }
//*******************************************************************************************************
// Part 2
// ======
// the traversal for each query should use the ForkJoin Framework effectively. The work will remain O(n),
// but the span should lower to O(log n). Finding the corners should require only one data traversal,
// and each query should require only one additional data traversal.
//*******************************************************************************************************
    public static void preprocessTwo(CensusData res, int x, int y) {
        CORNERS = cornerFind(res, 0, res.data_size);
        POPULATION = CORNERS.totalPop;
    }

    public static Pair<Integer, Float> versionTwo(CensusData res, int x, int y, int[] query) {
        int[] result = new int[2];
        int popCount = computePop(res, 0, res.data_size, CORNERS, query, x, y);
        float percentage = (float) popCount/POPULATION * 100;
        return new Pair<Integer, Float>(popCount, percentage);
    }
//--------------------------------------------------------------------------------
// Find the max and min Latitude/Longitude using ForkJoin Framework
//--------------------------------------------------------------------------------
    public static Corners cornerFind(CensusData res, int lo, int hi) {
        if (hi - lo < SEQUENTIAL_CUTOFF) {
            float maxLat = res.data[lo].latitude;
            float minLat = maxLat;
            float maxLong = res.data[lo].longitude;
            float minLong = maxLong;
            int population = 0;
            for (int i=lo; i<hi; ++i) {
                maxLat = max(res.data[i].latitude, maxLat);
                minLat = min(res.data[i].latitude, minLat);
                maxLong = max(res.data[i].longitude, maxLong);
                minLong = min(res.data[i].longitude, minLong);
                population += res.data[i].population;
            }
            return new Corners(maxLat, minLat, maxLong, minLong, population);
        } else {
            ForkJoinTask<Corners> left =
                ForkJoinTask.adapt(() -> cornerFind(res, lo, (hi+lo)/2)).fork();
            Corners rightAns = cornerFind(res, (hi+lo)/2, hi);
            Corners leftAns = left.join();
            return maxCorner(leftAns, rightAns);
        }
    }
//--------------------------------------------------------------------------------
// Helper function to return the correct return the max/min values given
// two Corners (basically making the biggest rectangle given two rectangles)
//--------------------------------------------------------------------------------
    public static Corners maxCorner(Corners left, Corners right) {
        if (left.consider == false && right.consider == false)
            return new Corners(false);
        if (left.consider == false)
            return right;
        if (right.consider == false)
            return left;
        return new Corners(max(left.maxLat, right.maxLat),
             min(left.minLat, right.minLat), max(left.maxLong, right.maxLong),
             min(left.minLong, right.minLong), left.totalPop + right.totalPop);
    }
//--------------------------------------------------------------------------------
// Compute the population using ForkJoin, each subproblem gets it's portion of
// the CensusGroup Array
//--------------------------------------------------------------------------------
    public static Integer computePop(CensusData res,
                int lo, int hi, Corners corners, int[] query, int x, int y) {
        if (hi - lo < SEQUENTIAL_CUTOFF) {
            int popCount = 0;
            for (int i = lo; i<hi; ++i) {
                int[] censusPos = censusToGrid(res.data[i], x, y,
                     corners.minLong, corners.maxLong, corners.minLat, corners.maxLat);
                if (inGrid(censusPos, query)) {
                    popCount = popCount + res.data[i].population;
                }          
            }
            return popCount;
        } else {
            ForkJoinTask<Integer> left =
                ForkJoinTask.adapt(() -> 
                    computePop(res, lo, (hi+lo)/2, corners, query, x, y)).fork();
            int rightAns = computePop(res, (hi+lo)/2, hi, corners, query, x, y);
            int leftAns = left.join();
            return leftAns + rightAns;
        }
    }
//*******************************************************************************************************
// Part 3
// ======
// This version will, like version 1, not use any parallelism, but it will perform additional preprocessing
//  so that each query can be answered in O(1) time.
//*******************************************************************************************************
    public static void preprocessThree(CensusData res, int x, int y) {
        float maxLat = res.data[0].latitude;
        float minLat = maxLat;
        float maxLong = res.data[0].longitude;
        float minLong = maxLong;
        GRID = new int [y][x];

        for (int i=0; i<res.data_size; ++i) {
            maxLat = max(res.data[i].latitude, maxLat);
            minLat = min(res.data[i].latitude, minLat);
            maxLong = max(res.data[i].longitude, maxLong);
            minLong = min(res.data[i].longitude, minLong);
        }
        for (int i = 0; i<res.data_size; ++i) {
            int[] xy = censusToGrid(res.data[i], x, y, minLong, maxLong, minLat, maxLat);
            int xval = xy[1] - 1;
            int yval = xy[0] - 1;
            GRID[xval][yval] += res.data[i].population;
            POPULATION += res.data[i].population;
        }
        transform(GRID, y, x);
        CORNERS = new Corners(maxLat, minLong, maxLong, minLong, POPULATION);
    }

    public static Pair<Integer, Float> versionThree(int x, int y, int[] query) {
        int popCount = popCountGrid(GRID, x, y, query);
        float percentage = (float) popCount/POPULATION * 100;
        return new Pair<Integer, Float>(popCount, percentage);
    }
//--------------------------------------------------------------------------------
// Transforming the grid into the modified 'unusual' version
//--------------------------------------------------------------------------------
    public static void transform(int[][] grid, int row, int col) {
        for (int i = 0; i < row; ++i) {
            for (int j = 0; j < col; ++j) {
                int left = get(grid, i, j-1, row, col);
                int up = get(grid, i-1, j, row, col);
                int upLeft = get(grid, i-1, j-1, row, col);
                grid[i][j] += left + up - upLeft;
            }
        }
    }
//--------------------------------------------------------------------------------
// Given the "modified" grid of version three, returning the population
// count by applying the four rules
//--------------------------------------------------------------------------------
    public static int popCountGrid(int[][] grid, int x, int y, int[] query) {
        int bottomRight = get(grid, query[3]-1, query[2]-1, y, x);
        int aboveTopRight = get(grid, query[1]-2, query[2]-1, y, x);
        int leftBottomLeft = get(grid, query[3]-1, query[0]-2, y, x);
        int upleftUpperLeft = get(grid, query[1]-2, query[0 ]-2, y, x);
        // System.out.printf("1:%d, 2:%d, 3:%d, 4:%d\n", bottomRight, aboveTopRight, leftBottomLeft, upleftUpperLeft);
        return bottomRight - aboveTopRight - leftBottomLeft + upleftUpperLeft;
    }
//*******************************************************************************************************
// Part 4
// ======
// Same as part 3 but the grid bulding phase has been parallelized
//*******************************************************************************************************
    public static void preprocessFour(CensusData res, int x, int y) {
        CORNERS = cornerFind(res, 0, res.data_size);
        POPULATION = CORNERS.totalPop;
        GRID = new int[y][x];
        buildGridParallel(res, GRID, 0, res.data_size, CORNERS, x, y);
        transform(GRID, y, x);        
    }
//--------------------------------------------------------------------------------
// Using ForkJoin to fill the initial grid with relevant populations. Every
// thread fills a specified portion of the grid. The returned grids are added
// in parallel
//--------------------------------------------------------------------------------
    public static void buildGridParallel(CensusData res, int[][] result,
                int lo, int hi, Corners corners, int x, int y) {
        if (hi - lo <= SEQUENTIAL_CUTOFF) {
            for (int i = lo; i<hi; ++i) {
                int[] xy = censusToGrid(res.data[i], x, y, 
                    corners.minLong, corners.maxLong, corners.minLat, corners.maxLat);
                int xval = xy[1] - 1;
                int yval = xy[0] - 1;
                result[xval][yval] += res.data[i].population;
            }
        } else {
            int[][] result1 = new int[y][x];
            ForkJoinTask<?> left = ForkJoinTask.adapt(()->
                    buildGridParallel(res, result1, lo, (hi+lo)/2, corners, x, y)).fork();
            int[][] result2 = new int[y][x];
            buildGridParallel(res, result2, (hi+lo)/2, hi, corners, x, y);
            left.join();
            addGridFork(result, result1, result2, 0, y, 0, x);
        }
    }
//--------------------------------------------------------------------------------
// Adding two grids in parallel. Every thread gets a portion of the grid.
// Using two different sequential cutoffs for rows and columns
//--------------------------------------------------------------------------------
    public static void addGridFork(int[][] result,int[][] result1, int[][] result2, int rowBegin, 
        int rowEnd, int colBegin, int colEnd) 
     {
        if (rowEnd - rowBegin <= SEQUENTIAL_CUTOFF_ROWS) 
        {
            if (colEnd - colBegin <= SEQUENTIAL_CUTOFF_COLS) 
                for (int i = rowBegin; i < rowEnd; ++i)
                    for (int j = colBegin; j < colEnd; ++j)
                        result[i][j] = result1[i][j] + result2[i][j];
            else {
                ForkJoinTask<?> left = ForkJoinTask.adapt(()-> addGridFork(result, result1, result2, rowBegin, 
                    rowEnd, colBegin, (colBegin+colEnd)/2)).fork();
                addGridFork(result, result1, result2, rowBegin, rowEnd, (colBegin+colEnd)/2, colEnd);
                left.join();
            }
        } 
        else 
        {
            ForkJoinTask<?> left = ForkJoinTask.adapt(()-> addGridFork(result, result1, result2, rowBegin, 
                (rowBegin+rowEnd)/2, colBegin, colEnd)).fork();
            addGridFork(result, result1, result2, (rowBegin+rowEnd)/2, rowEnd, colBegin, colEnd);
            left.join();
        }
    }
//*******************************************************************************************************
// Part 5
// ======
// Same concept as part 3 but the grid is build using shared GRID and locks with threads
//*******************************************************************************************************
    public static void preprocessFive(CensusData res, int x, int y) {
        CORNERS = cornerFind(res, 0, res.data_size);
        POPULATION = CORNERS.totalPop;
        Object[][] locks = new Object[y][x];
        initLocks(locks, 0, y, 0, x);
        GRID = new int[y][x];
        ExecutorService pool = Executors.newFixedThreadPool(THREADS);
        int part_size = res.data_size/THREADS;
        for (int i = 0; i<THREADS; ++i) {
            final int tid = i;
            pool.execute(()-> {
                for (int j = tid*part_size; j<(tid+1)*part_size; ++j) {
                    int[] xy = censusToGrid(res.data[j], x, y,
                    CORNERS.minLong, CORNERS.maxLong, CORNERS.minLat, CORNERS.maxLat);
                    int xval = xy[1] - 1;
                    int yval = xy[0] - 1;
                    synchronized(locks[xval][yval]) {
                        GRID[xval][yval] += res.data[j].population;
                    }
                }
            });
        }
        pool.shutdown();
        try {
            pool.awaitTermination(60, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            System.out.println
                ("preprocessFive -> Thread didn't finish work in 60 seconds. Increase Timeout if job size too big");
        }
        transform(GRID, y, x);    
    }
//--------------------------------------------------------------------------------
// Initializing the Grid of locks in parallel (as objects have null as default)
// Only at very large grid sizes is the speedup significant
//--------------------------------------------------------------------------------
    static void initLocks(Object[][] locks, int rowBegin, int rowEnd, int colBegin, int colEnd) {
        if (rowEnd - rowBegin <= SEQUENTIAL_CUTOFF_ROWS) {
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
        else {
            ForkJoinTask<?> left = ForkJoinTask.adapt(()-> initLocks(locks, rowBegin, 
                (rowBegin+rowEnd)/2, colBegin, colEnd)).fork();
            initLocks(locks, (rowBegin+rowEnd)/2, rowEnd, colBegin, colEnd);
            left.join();
        }
    }
// **********************************************************************************
// HELPER FUNCTIONS
// **********************************************************************************
/*
    Method to abstract away the detail of getting values from 2d grid
    Warning: Will return a zero if index out of bounds, so only use where
    appropriate
*/
    public static int get(int[][] grid, int getx, int gety, int row, int col) {
        if (getx < 0 || getx >= row || gety < 0 || gety >= col) {
            return 0;
        }
        return grid[getx][gety];
    }  
    public static void printArr(int[][] arr, int row, int col) {
        for (int i = 0; i < row; ++i)
            System.out.println(Arrays.toString(arr[i]));
    }
    public static int[] getQuery() {
        try {
        System.out.println("Enter you query as four integers separated by space");
        String s = BR.readLine();
        String[] q = s.split(" ");
        if (q.length != 4) {
            System.exit(1);
        }
        return Stream.of(q).mapToInt(Integer::parseInt).toArray();
    } catch (IOException e) {
        e.printStackTrace();
        System.exit(-1);
        return null;
        }
    }
/*
    Computing the grid number of a census block an array [x, y] where x and y
    are the columns and rows of the grid. The result is one indexed for now
*/
    public static int[] censusToGrid(CensusGroup group, int x, int y,
            float minLong, float maxLong, float minLat, float maxLat) {
        int[] retval = new int[2];
        float longRange = maxLong - minLong;
        float latRange = maxLat - minLat;
        float xval = (group.longitude - minLong)/longRange * x + 1;
        float yval = (group.latitude - minLat)/latRange * y + 1;
        if (group.latitude == maxLat) {
            yval = yval-1;
        }
        if (group.longitude == maxLong) {
            xval = xval-1;
        }
        retval[0] = (int) xval;
        retval[1] = (int) yval;
        return retval;
    }
/*
    Checking whether the census block lies in the query
*/
    public static boolean inGrid(int[] census, int[] query) {
        if (census[0] >= query[0] && census[0] <= query[2]
                && census[1] >= query[1] && census[1] <= query[3]) {
            return true;
        }
        return false;
    }
    public static boolean isCorrectQuery(int[] query, int x, int y) {
        if (query[0] < 1 || query[0] > x)
            return false;
        if (query[1] < 1 || query[1] > y)
            return false;
        if (query[2] < query[0] || query[2] > x)
            return false;
        if (query[3] < query[1] || query[3] > y)
            return false;
        return true;
    }

//***************************************************************************************************
// Functions for integration with the GUI
//***************************************************************************************************
    public static void preprocess(String filename, int x, int y, int versionNum) {
        result = parse(filename);
        X = x;
        Y = y;
        VERSION = versionNum;
        POPULATION = 0;
        switch(versionNum) {
            case 1 : preprocessOne(result, x, y);     break;
            case 2 : preprocessTwo(result, x, y);     break;
            case 3 : preprocessThree(result, x, y);        break;
            case 4 : preprocessFour(result, x, y);         break;
            case 5 : preprocessFive(result, x, y);         break;
            default: System.out.println("Illegal version given. Exiting Program");
        }
    }
    public static Pair<Integer, Float> singleInteraction(int w, int s, int e, int n) {
        int[] query = {w, s, e, n};
        switch(VERSION) {
            case 1 : return versionOne(result, X, Y, query);
            case 2 : return versionTwo(result, X, Y, query);
            case 3 : return versionThree(X, Y, query);
            case 4 : return versionThree(X, Y, query);
            case 5 : return versionThree(X, Y, query);
            default: System.out.println("Illegal version given. Exiting Program");
        }
        return null;
    }
}









