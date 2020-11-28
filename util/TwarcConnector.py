import logging
import random
import time
from multiprocessing import Lock

from twarc import Twarc


class TwarcConnector:

    def __init__(self, key_file, time_window=900, window_limit=15):
        self._lock = Lock()
        self.val = None
        self.streams = []
        self._init_twarc_objects(key_file)
        self.timers = dict()
        self.time_window = time_window
        self.window_limit = window_limit

        for i in range(0, len(self.streams)):
            self.timers[i] = [0, 0]

        random.shuffle(self.streams)

    def __str__(self):
        return repr(self) + self.val

    def _init_twarc_objects(self, keys_file):
        """
        Reads the keys file and initiates an array of twython objects
        :param keys_file: Twitter keys file
        :return:
        """
        with open(keys_file, 'r') as fKeysIn:
            for line in fKeysIn:
                line = line.rstrip().split('\t')
                try:
                    conect = self._get_twitter_connection(app_key=line[0], app_secret=line[1],
                                                 oauth_token=line[2], oauth_token_secret=line[3])
                    self.streams.append(conect)
                except:
                    continue


        logging.info("Initialized TwarcthonConnector")
        logging.info("There are {} instances".format(len(self.streams)))
    @staticmethod
    def _get_twitter_connection(app_key=None, app_secret=None, oauth_token=None,
                                oauth_token_secret=None):

            return Twarc(app_key, app_secret, oauth_token, oauth_token_secret)


    def change_params(self, window_limit, time_window):
        self.time_window = time_window
        self.window_limit = window_limit

    def get_twarc_connection(self):
        """
        Returns the twython object for making the requests and sleeps if all the twitter keys have reached the usage
        limits
        :return: twarc object for making API calls
        """
        result = -1
        max_sleep_time = self.time_window
        with self._lock:
            while result == -1:
                for i in range(0, len(self.streams)):
                    curr_sleep_time = max((self.timers[i][0] + self.time_window) - time.time(), 0)

                    max_sleep_time = min(max_sleep_time, curr_sleep_time)

                    if self.timers[i][1] >= self.window_limit and self.timers[i][0] + self.time_window < time.time():
                        self.timers[i][0] = 0
                        self.timers[i][1] = 0
                        continue

                    if self.timers[i][1] < self.window_limit:
                        result = i
                        break

                if result == -1:  # case when all streams are rate limited
                    # time.sleep(300)
                    logging.warning('sleeping for %d seconds.' % max_sleep_time)
                    time.sleep(max_sleep_time)

            if self.timers[result][0] == 0:
                self.timers[result][0] = time.time()

            self.timers[result][1] += 1
            # print("Result %d - counter %d  obj: %s" % (result, self.timers[result][1], self.streams[result].oauth_token))
            logging.info("Change to {} Twarc Instance {}".format(result, self.timers[result][1]))
            return self.streams[result]


    def get_twarc_connection_new(self, idx=None, error=None):
        """
        Returns the twython object for making the requests and sleeps if all the twitter keys have reached the usage
        limits
        :return: twarc object for making API calls
        """
        # timestap = sleep_time + time.time()

        if idx is not None and error is not None:
            sleep_time = float(str(error))
            wakeup_time = sleep_time + time.time()
            self.timers[idx][0] = wakeup_time

        result = -1
        max_sleep_time = self.time_window
        with self._lock:
            while result == -1:

                for i in range(0, len(self.streams)):
                    curr_sleep_time = max(self.timers[i][0] - time.time(), 0)
                    if curr_sleep_time == 0:
                        result = i
                        break
                    max_sleep_time = min(max_sleep_time, curr_sleep_time)


                if result == -1:  # case when all streams are rate limited
                    # time.sleep(300)
                    logging.warning('sleeping for %d seconds.' % max_sleep_time)
                    time.sleep(max_sleep_time)


            # print("Result %d - counter %d  obj: %s" % (result, self.timers[result][1], self.streams[result].oauth_token))
            logging.info("Change to {} Twarc Instance {}".format(result, self.timers[result][0]))
            return self.streams[result], result

