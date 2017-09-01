public class Corners {
	boolean consider = false;
	public float maxLat;
	public float minLat;
	public float maxLong;
	public float minLong;
	public int totalPop;

	public Corners(float a, float b, float c, float d, int p) {
		maxLat = a;
		minLat = b;
		maxLong = c;
		minLong = d;
		totalPop = p;
		consider = true;
	}
	public Corners(boolean con) {
		consider = false;
	}
}