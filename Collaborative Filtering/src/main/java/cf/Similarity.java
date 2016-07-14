package cf;

/*
 * Implement data structure and respective interface for similarity storage
 */

public class Similarity{
	protected int Item1 = 0;
	protected int Item2 = 0;
	protected double similarity = 0.0;
	protected double Item2Score = 0.0;

	public Similarity() {

	}

	public Similarity(int Item1) {
		this.Item1 = Item1;
	}

	public int getItem1() {
		return Item1;
	}
	public void setItem1(int Item1) {
		this.Item1 = Item1;
	}

	public int getItem2() {
		return Item2;
	}
	public void setItem2(int Item2) {
		this.Item2 = Item2;
	}
	
	public double getSimilar() {
		return similarity;
	}
	public void setSimilar(double similarity) {
		this.similarity = similarity;
	}
	
	public double getItem2Score() {
		return Item2Score;
	}
	public void setItem2Score(double Item2Score) {
		this.Item2Score = Item2Score;
	}
}