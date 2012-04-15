"""MapReduce job for calculating review weights.
"""
from mrjob.job import MRJob


class R_Score:
  Like, Neutral, Dislike = xrange(3)

class ReviewerWeightCalc(MRJob):
  
  #my_hash keys are business guids, values are R_Scores
  def __init__(self, *args, **kwargs):
    super(ReviewerWeightCalc, self).__init__(*args, **kwargs)
    #TODO: self._my_hash = my_hash
    self._my_hash = {
                      "biz1234" : 1,
                      "biz1444" : 4,
                      "biz4444" : 3
                    }

  @staticmethod
  def rel_score(score):
    if score >= 4:
      return R_Score.Like
    elif score == 3:
      return R_Score.Neutral
    else:
      return R_Score.Dislike

  @staticmethod
  def is_mismatch(score1, score2):
    #Like = 0, Dislike = 2
    if abs(score1-score2) == 2:
      return True
    else:
      return False

  #assume row is space separated string for now: user guid score
  #We should only pass reviews in that are of the same high level category 
  def mapper(self, _, row):
    (user, guid, score) = row.split(" ")
    if guid in self._my_hash:
      their_relative_score = ReviewerWeightCalc.rel_score(int(score))
      my_score = ReviewerWeightCalc.rel_score(self._my_hash[guid])
      if my_score == their_relative_score:
        yield (user, 1)
      elif ReviewerWeightCalc.is_mismatch(my_score, 
                                          their_relative_score):
        yield (user, -1)

  def combiner(self, user, weights):
    yield (user, sum(weights))

  def reducer(self, user, weights):
    yield (user, sum(weights))


if __name__ == '__main__':
  ReviewerWeightCalc.run()
