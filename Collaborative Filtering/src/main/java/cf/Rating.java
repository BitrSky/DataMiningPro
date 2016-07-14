package cf;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

/*
 * Rating.java - the four rating will be executed
 */

public class Rating{
	
	//the K value
	static int K = 100;
	
	//the user-user rating algorithm
	public static void rating_user(ArrayList<QueryPair> QuerySet, ArrayList<ArrayList<DataTuple>> UserFirstRating){
		ArrayList <Similarity> SimList = new ArrayList <Similarity>();
		
		for(QueryPair CurQuery : QuerySet){
			//get requested user info
			ArrayList<DataTuple> QueryUserInfo = new ArrayList<DataTuple>();
			SimList = new ArrayList <Similarity>();

			for(ArrayList<DataTuple> usr : UserFirstRating){		
				if(usr.get(0).getUser() == CurQuery.getUser()){
					for(DataTuple tmp : usr){
						QueryUserInfo.add(tmp);
					}
					break;
				}
			}
//			for(DataTuple tmp : QueryUserInfo)
//				System.out.println(tmp.getUser()+"  "+tmp.getMovie()+"  "+tmp.getRating());
			
			int zeroflag = 0;
			//compute similarity between user in query and every other query
			for(ArrayList<DataTuple> CompareUsr : UserFirstRating){
				if(CompareUsr.get(0).getUser() != QueryUserInfo.get(0).getUser()){
					Similarity CurSmiliar = new Similarity();

					int existflag = 0;
					for(DataTuple tmp : CompareUsr){
						if(tmp.getMovie() == CurQuery.getMovie()){
							existflag = 1;
							CurSmiliar.setItem2Score(tmp.getRating());
							break;
						}
					}
					
					if(existflag == 0)
						continue;
					
					int p1 = 0;
					int p2 = 0;
					while((p1 < QueryUserInfo.size()) && (p2 < CompareUsr.size())){
						
						if(QueryUserInfo.get(p1).getMovie() == CompareUsr.get(p2).getMovie()){
							CurSmiliar.setItem1(QueryUserInfo.get(p1).getUser());
							CurSmiliar.setItem2(CompareUsr.get(p2).getUser());
							CurSmiliar.setSimilar(CurSmiliar.getSimilar() + QueryUserInfo.get(p1).getRating() * CompareUsr.get(p2).getRating());
							p1++;
							p2++;
							continue;
						}
						
						if(QueryUserInfo.get(p1).getMovie() < CompareUsr.get(p2).getMovie()){
							p1++;
							continue;
						}
						
						if(QueryUserInfo.get(p1).getMovie() > CompareUsr.get(p2).getMovie()){
							p2++;
							continue;
						}
					}
					if (CurSmiliar.getSimilar() != 0.0){
						zeroflag = 1;
					}
					SimList.add(CurSmiliar);
				}
			}
			SimList.trimToSize();
			
			Rankcomparator comp = new Rankcomparator();
			Collections.sort(SimList, comp);
			
//			for(int i = 0; i < Math.min(K, SimList.size()); i++)
//				System.out.println(SimList.get(i).getItem1()+"  "+SimList.get(i).getItem2()+"  "+SimList.get(i).getSimilar() +"  "+SimList.get(i).getItem2Score());
			
			//predict query score with user neighbors' score
			double totalscore = 0;
			double totalweight = 0;
			for(int i = 0; i < Math.min(K, SimList.size()); i++){
				totalweight += SimList.get(i).getSimilar();
				
				if(zeroflag == 0){
					totalscore += SimList.get(i).getItem2Score();
				}
				else{
					totalscore += SimList.get(i).getSimilar() * SimList.get(i).getItem2Score();
				}
			}
			if(zeroflag == 0){
				CurQuery.setRating((double) totalscore / (double) Math.min(K, SimList.size()));
			}
			else{
				CurQuery.setRating(totalscore / totalweight);
			}
		}
	}
	
	//the movie-movie rating algorithm
	public static void rating_movie(ArrayList<QueryPair> QuerySet, ArrayList<ArrayList<DataTuple>> MovieFirstRating){
		ArrayList <Similarity> SimList = new ArrayList <Similarity>();
		
		for(QueryPair CurQuery : QuerySet){
			//get requested movie info
			ArrayList<DataTuple> QueryMovieInfo = new ArrayList<DataTuple>();
			SimList = new ArrayList <Similarity>();

			for(ArrayList<DataTuple> mov : MovieFirstRating){
				if(mov.get(0).getMovie() == CurQuery.getMovie()){
					for(DataTuple tmp : mov){
						QueryMovieInfo.add(tmp);
					}
					break;
				}
			}
//			for(DataTuple tmp : QueryUserInfo)
//				System.out.println(tmp.getUser()+"  "+tmp.getMovie()+"  "+tmp.getRating());
			
			int zeroflag = 0;
			//compute similarity between user in query and every other query
			for(ArrayList<DataTuple> CompareMov : MovieFirstRating){
				if(CompareMov.get(0).getMovie() != QueryMovieInfo.get(0).getMovie()){
					Similarity CurSmiliar = new Similarity();

					int existflag = 0;
					for(DataTuple tmp : CompareMov){
						if(tmp.getUser() == CurQuery.getUser()){
							existflag = 1;
							CurSmiliar.setItem2Score(tmp.getRating());
							break;
						}
					}
					
					if(existflag == 0)
						continue;
					
					int p1 = 0;
					int p2 = 0;
					while((p1 < QueryMovieInfo.size()) && (p2 < CompareMov.size())){
						
						if(QueryMovieInfo.get(p1).getUser() == CompareMov.get(p2).getUser()){
							CurSmiliar.setItem1(QueryMovieInfo.get(p1).getMovie());
							CurSmiliar.setItem2(CompareMov.get(p2).getMovie());
							CurSmiliar.setSimilar(CurSmiliar.getSimilar() + QueryMovieInfo.get(p1).getRating() * CompareMov.get(p2).getRating());
							p1++;
							p2++;
							continue;
						}
						
						if(QueryMovieInfo.get(p1).getUser() < CompareMov.get(p2).getUser()){
							p1++;
							continue;
						}
						
						if(QueryMovieInfo.get(p1).getUser() > CompareMov.get(p2).getUser()){
							p2++;
							continue;
						}
					}
					if (CurSmiliar.getSimilar() != 0.0){
						zeroflag = 1;
					}
					SimList.add(CurSmiliar);
				}
			}
			SimList.trimToSize();
			
			Rankcomparator comp = new Rankcomparator();
			Collections.sort(SimList, comp);
			
//			for(int i = 0; i < Math.min(K, SimList.size()); i++)
//				System.out.println(SimList.get(i).getItem1()+"  "+SimList.get(i).getItem2()+"  "+SimList.get(i).getSimilar() +"  "+SimList.get(i).getItem2Score());
			
			//predict query score with movie neighbors' score
			double totalscore = 0;
			double totalweight = 0;
			for(int i = 0; i < Math.min(K, SimList.size()); i++){
				totalweight += SimList.get(i).getSimilar();
				
				if(zeroflag == 0){
					totalscore += SimList.get(i).getItem2Score();
				}
				else{
					totalscore += SimList.get(i).getSimilar() * SimList.get(i).getItem2Score();
				}
			}
			if(zeroflag == 0){
				CurQuery.setRating((double) totalscore / (double) Math.min(K, SimList.size()));
			}
			else{
				CurQuery.setRating(totalscore / totalweight);
			}
		}
	}
	
	//the user-user norm algorithm
	public static void rating_user_norm(ArrayList<QueryPair> QuerySet, ArrayList<ArrayList<DataTuple>> UserFirstRating){
		ArrayList <Similarity> SimList = new ArrayList <Similarity>();
		
		//get average score of all users
		ArrayList <Similarity> UsrAvr = new ArrayList <Similarity>();
		double avrall = 0.0;
		double countall = 0.0;
		for(ArrayList<DataTuple> tmpusr : UserFirstRating){
			int total = 0;
			for(DataTuple tmppair : tmpusr){
				total += tmppair.getRating();
				countall++;
				avrall += tmppair.getRating();
			}
			Similarity a = new Similarity(tmpusr.get(0).getUser());
			double avr = (double) total / (double) tmpusr.size();
			a.setSimilar(avr);
			UsrAvr.add(a);
		}
		avrall = avrall / countall;
		
		for(QueryPair CurQuery : QuerySet){
			//get requested user info
			ArrayList<DataTuple> QueryUserInfo = new ArrayList<DataTuple>();
			SimList = new ArrayList <Similarity>();

			for(ArrayList<DataTuple> usr : UserFirstRating){		
				if(usr.get(0).getUser() == CurQuery.getUser()){
					for(DataTuple tmp : usr){
						QueryUserInfo.add(tmp);
					}
					break;
				}
			}
//			for(DataTuple tmp : QueryUserInfo)
//				System.out.println(tmp.getUser()+"  "+tmp.getMovie()+"  "+tmp.getRating());
			
			int zeroflag = 0;
			//compute similarity between user in query and every other query
			for(ArrayList<DataTuple> CompareUsr : UserFirstRating){
				if(CompareUsr.get(0).getUser() != QueryUserInfo.get(0).getUser()){
					Similarity CurSmiliar = new Similarity();

					int existflag = 0;
					for(DataTuple tmp : CompareUsr){
						if(tmp.getMovie() == CurQuery.getMovie()){
							existflag = 1;
							CurSmiliar.setItem2Score(tmp.getRating());
							break;
						}
					}
					
					if(existflag == 0)
						continue;
					
					int p1 = 0;
					int p2 = 0;
					while((p1 < QueryUserInfo.size()) && (p2 < CompareUsr.size())){
						
						if(QueryUserInfo.get(p1).getMovie() == CompareUsr.get(p2).getMovie()){
							CurSmiliar.setItem1(QueryUserInfo.get(p1).getUser());
							CurSmiliar.setItem2(CompareUsr.get(p2).getUser());
							CurSmiliar.setSimilar(CurSmiliar.getSimilar() + QueryUserInfo.get(p1).getRating() * CompareUsr.get(p2).getRating());
							p1++;
							p2++;
							continue;
						}
						
						if(QueryUserInfo.get(p1).getMovie() < CompareUsr.get(p2).getMovie()){
							p1++;
							continue;
						}
						
						if(QueryUserInfo.get(p1).getMovie() > CompareUsr.get(p2).getMovie()){
							p2++;
							continue;
						}
					}
					if (CurSmiliar.getSimilar() != 0.0){
						zeroflag = 1;
					}
					SimList.add(CurSmiliar);
				}
			}
			SimList.trimToSize();
			
			Rankcomparator comp = new Rankcomparator();
			Collections.sort(SimList, comp);
			
//			for(int i = 0; i < Math.min(K, SimList.size()); i++)
//				System.out.println(SimList.get(i).getItem1()+"  "+SimList.get(i).getItem2()+"  "+SimList.get(i).getSimilar() +"  "+SimList.get(i).getItem2Score());
			
			//predict query score with movie neighbors' "normed" score
			double totalscore = 0.0;
			double totalweight = 0.0;
			for(int i = 0; i < Math.min(K, SimList.size()); i++){
				double curscore = 0;
				for (Similarity tmp : UsrAvr){
					if(tmp.getItem1() == SimList.get(i).getItem2()){
						curscore = SimList.get(i).getItem2Score() - tmp.getSimilar() + avrall;
					}
				}
				
				totalweight += SimList.get(i).getSimilar();
				
				if(zeroflag == 0){
					totalscore += curscore;
				}
				else{
					totalscore += SimList.get(i).getSimilar() * curscore;
				}
			}
			if(zeroflag == 0){
				CurQuery.setRating((double) totalscore / (double) Math.min(K, SimList.size()));
			}
			else{
				CurQuery.setRating(totalscore / totalweight);
			}
		}
	}

	//the custom algorithm
//	public static void rating_custom(ArrayList<QueryPair> QuerySet, ArrayList<ArrayList<DataTuple>> UserFirstRating){
//		ArrayList <Similarity> SimList = new ArrayList <Similarity>();
//
//		//get average score of all users
//		ArrayList <Similarity> UsrAvr = new ArrayList <Similarity>();
//		for(ArrayList<DataTuple> tmpusr : UserFirstRating){
//			int total = 0;
//			for(DataTuple tmppair : tmpusr){
//				total += tmppair.getRating();
//			}
//			Similarity a = new Similarity(tmpusr.get(0).getUser());
//			double avr = (double) total / (double) tmpusr.size();
//			a.setSimilar(avr);
//			UsrAvr.add(a);
//		}
//
//		for(QueryPair CurQuery : QuerySet){
//			double curusravr = 0.0;
//			//get requested user info
//			ArrayList<DataTuple> QueryUserInfo = new ArrayList<DataTuple>();
//			SimList = new ArrayList <Similarity>();
//
//			for(ArrayList<DataTuple> usr : UserFirstRating){
//				if(usr.get(0).getUser() == CurQuery.getUser()){
//					for(DataTuple tmp : usr){
//						curusravr += tmp.getRating();
//						QueryUserInfo.add(tmp);
//					}
//					curusravr = curusravr / (double) usr.size();
//					break;
//				}
//			}
////			for(DataTuple tmp : QueryUserInfo)
////				System.out.println(tmp.getUser()+"  "+tmp.getMovie()+"  "+tmp.getRating());
//
//			//compute similarity between user in query and every other query
//			int zeroflag = 0;
//			for(ArrayList<DataTuple> CompareUsr : UserFirstRating){
//				if(CompareUsr.get(0).getUser() != QueryUserInfo.get(0).getUser()){
//					Similarity CurSmiliar = new Similarity();
//
//					int existflag = 0;
//					for(DataTuple tmp : CompareUsr){
//						if(tmp.getMovie() == CurQuery.getMovie()){
//							existflag = 1;
//							CurSmiliar.setItem2Score(tmp.getRating());
//							break;
//						}
//					}
//
//					if(existflag == 0)
//						continue;
//
//					int p1 = 0;
//					int p2 = 0;
//					double norm1 = 0.0;
//					double norm2 = 0.0;
//					while((p1 < QueryUserInfo.size()) && (p2 < CompareUsr.size())){
//
//						if(QueryUserInfo.get(p1).getMovie() == CompareUsr.get(p2).getMovie()){
//							CurSmiliar.setItem1(QueryUserInfo.get(p1).getUser());
//							CurSmiliar.setItem2(CompareUsr.get(p2).getUser());
//							CurSmiliar.setSimilar(CurSmiliar.getSimilar() + QueryUserInfo.get(p1).getRating() * CompareUsr.get(p2).getRating());
//							norm1 += Math.pow(QueryUserInfo.get(p1).getRating(), 2);
//							norm2 += Math.pow(CompareUsr.get(p2).getRating(), 2);
//							p1++;
//							p2++;
//							continue;
//						}
//
//						if(QueryUserInfo.get(p1).getMovie() < CompareUsr.get(p2).getMovie()){
//							norm1 += Math.pow(QueryUserInfo.get(p1).getRating(), 2);
//							p1++;
//							continue;
//						}
//
//						if(QueryUserInfo.get(p1).getMovie() > CompareUsr.get(p2).getMovie()){
//							norm2 += Math.pow(CompareUsr.get(p2).getRating(), 2);
//							p2++;
//							continue;
//						}
//					}
//					if (CurSmiliar.getSimilar() != 0.0){
//						zeroflag = 1;
//						CurSmiliar.setSimilar(CurSmiliar.getSimilar() / Math.sqrt(norm1 * norm2));
//					}
//					else{
//						CurSmiliar.setSimilar(CurSmiliar.getSimilar());
//					}
//
//					SimList.add(CurSmiliar);
//				}
//			}
//			SimList.trimToSize();
//
//			Rankcomparator comp = new Rankcomparator();
//			Collections.sort(SimList, comp);
//
////			for(int i = 0; i < Math.min(K, SimList.size()); i++)
////				System.out.println(SimList.get(i).getItem1()+"  "+SimList.get(i).getItem2()+"  "+SimList.get(i).getSimilar() +"  "+SimList.get(i).getItem2Score());
//
//			//predict query score with movie neighbors' "normed" score
//			double totalscore = 0.0;
//			double totalweight = 0.0;
//			for(int i = 0; i < Math.min(K, SimList.size()); i++){
//				double curscore = 0;
//
//				for (Similarity tmp : UsrAvr){
//					if(tmp.getItem1() == SimList.get(i).getItem2()){
//						curscore = SimList.get(i).getItem2Score() - tmp.getSimilar() + curusravr;
//					}
//				}
//
//				totalweight += SimList.get(i).getSimilar();
//
//				if(zeroflag == 0){
//					totalscore += curscore;
//				}
//				else{
//					totalscore += SimList.get(i).getSimilar() * curscore;
//				}
//			}
//			if(zeroflag == 0){
//				CurQuery.setRating((double) totalscore / (double) Math.min(K, SimList.size()));
//			}
//			else{
//				CurQuery.setRating(totalscore / totalweight);
//			}
//		}
//	}

	public static class Rankcomparator implements Comparator<Object>{
		//Comparator for Similarity
		//the first sort key is Similarity
		//the second sort key is Item1

	    public int compare(Object o1,Object o2){
	    	Similarity sim1 = (Similarity)o1;
	    	Similarity sim2 = (Similarity)o2;
	    	if(sim1.getSimilar() < sim2.getSimilar())
	    		return 1;
	    	else if (sim1.getSimilar() > sim2.getSimilar())
	    		return -1;
	    	else if (sim1.getSimilar() == sim2.getSimilar()){
	    		if (sim1.getItem2() > sim2.getItem2())
	        	   return 1;
	    	}
	    	return -1;
	    }
	}
}