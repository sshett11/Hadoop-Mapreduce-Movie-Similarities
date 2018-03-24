# -*- coding: utf-8 -*-
# -*- coding: utf-8 -*-
from mrjob.job import MRJob
from mrjob.step import MRStep
from itertools import combinations
import numpy
from scipy import spatial
class movies_count(MRJob):
    
    def configure_args(self):
       
        super(movies_count, self).configure_args()
        self.add_passthru_arg(
                '-m', '--moviename', action="append", type='str', default=[], help='Expressions to search for.')
        self.add_passthrough_option(
                '-p', '--rating_pairs', type='int', default=1, help='minimum rating pairs')
        self.add_passthrough_option(
                '-k', '--items', type='int', default=25, help='number of items to looks for')
    
    def steps(self):
        return [
            MRStep(mapper=self.moviedatasplit,
                reducer=self.joinfilereducer),
          MRStep(reducer=self.reducer_moviepairs),
          MRStep(reducer=self.reducer_pairs),
          MRStep(reducer=self.movie_similarity)
    ]
   
 # Passing two files (movies.csv and ratings.csv) to the first mapper  
    def moviedatasplit(self, _, line):
        
                dsplit = line.split(",")
                if (len(dsplit) == 3): # movie data 
                    
                          yield dsplit[0], dsplit[1]
                else: # rating data
                    
                          yield dsplit[1], (dsplit[0], dsplit[2])
# generating user id as key and movie title, movierating as values with the help of first reducer
    def joinfilereducer(self, _, values):
                movielist = list(values)
                movietitle = movielist[0]
                tuplevalue = movielist[1:]
                for val in tuplevalue:
                        userid = val[0]
                        movierating = val[1]
                        
                        yield userid, (movietitle, movierating)      
# generating combination of two movies as key and their respective ratings  as value for each user id with the second reducer   
    def reducer_moviepairs(self,userid,values):
         for pair1,pair2 in combinations(values,2):
            title1=pair1[0]
            rating1=pair1[1]
            title2=pair2[0]
            rating2=pair2[1]
            yield (title1,title2),(rating1,rating2)
# combining all the ratings for each movie pair by different users with the third reducer     
    def reducer_pairs(self,titles,ratings):
        rating=[]
        for r in ratings:
            rating.append(r)
        yield titles,rating
            
# finding similarity  between movies
    def movie_similarity(self,titles,ratings):     
        rating =list(ratings)
        for ratings in rating:
            n=len(ratings)
        q1=[]
        q2=[]
        for r1 in ratings:
            q1.append((float(r1[0])))
            q2.append((float(r1[1])))
            
        if(n>self.options.rating_pairs):           
               for movie in self.options.moviename:
            
                  cor = numpy.corrcoef(q1,q2)[0,1]
                  cos_cor = 1-spatial.distance.cosine(q1,q2)
                  avg_cor = 0.5*(cor+cos_cor)
                   
                  if titles[0] == movie:
                   yield titles[0],(titles[1],avg_cor,cor,cos_cor,n)
                  elif titles[1]==movie:
                    yield titles[1], (titles[0],avg_cor,cor,cos_cor,n)
               
                    
if __name__ == '__main__':
            movies_count.run()

