package cf;

/*
 * Implement data structure and respective interface for score tuple storage
 * This class contains Movie, User and Rating member for user rating info.
 */

public class DataTuple {
	protected int Movie = 0;
	protected int User = 0;
	protected double Rating = 0.0;

	public DataTuple() {

	}

	public DataTuple(int Movie) {
		this.Movie = Movie;
	}

	public int getMovie() {
		return Movie;
	}
	public void setMovie(int Movie) {
		this.Movie = Movie;
	}

	public int getUser() {
		return User;
	}
	public void setUser(int User) {
		this.User = User;
	}
	
	public double getRating() {
		return Rating;
	}
	public void setRating(double Rating) {
		this.Rating = Rating;
	}
}