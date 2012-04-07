"""MapReduce job for calculating review weights.
"""
from mrjob.job import MRJob

class ReviewerWeightCalc(MRJob):
  
  #my_hash keys are business guids, values are R_Scores
  def __init__(self, *args, **kwargs):
    super(ReviewerWeightCalc, self).__init__(*args, **kwargs)
    #TODO: self._my_hash = my_hash
    self._my_hash = {
                      "biz1234" : "Dislike",
                      "biz1444" : "Like",
                      "biz4444" : "Neutral"
                    }

  #For now we use strings, ints might be faster:
  #class R_Scores:
  #  Like, Neutral, Dislike = range(3)
  @staticmethod
  def rel_score(score):
    if score >= 4:
      return "Like"
    elif score == 3:
      return "Neutral"
    else:
      return "Dislike"

  @staticmethod
  def is_mismatch(score1, score2):
    if( (score1 == "Like" and score2 == "Dislike") or
      (score1 == "Dislike" and score2 == "Like")):
      return True
    else:
      return False

  #assume row is space separated string for now: user guid score
  #We should only pass reviews in that are of the same high level category 
  def mapper(self, _, row):
    (user, guid, score) = row.split(" ")
    if guid in self._my_hash:
      their_relative_score = ReviewerWeightCalc.rel_score(int(score))
      if self._my_hash[guid] == their_relative_score:
        yield (user, 1)
      elif ReviewerWeightCalc.is_mismatch(self._my_hash[guid], 
                                       their_relative_score):
        yield (user, -1)

  def combiner(self, user, weights):
    yield (user, sum(weights))

  def reducer(self, user, weights):
    yield (user, sum(weights))


if __name__ == '__main__':
  ReviewerWeightCalc.run()
