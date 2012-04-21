"""
contains processes that can be subclassed for various workflows
"""
from multiprocessing import Process, Queue
from contextlib import contextmanager
import logging
import time


class KillableProcess(Process):
    """
    protocol to have a chance to cleanup when terminate is called

    example:

       while self._should_continue():
           do_stuff()
       else:
           self._all_done()
    """
    def __init__(self):
        Process.__init__(self)
        self.log = logging.getLogger(self.__class__.__name__)
        self.log.debug("initializing")

        #REFACTOR: might be able to move these into a contextmanager
        self._stop_queue = Queue()
        self._stopped_queue = Queue()

    @contextmanager
    def context(self):
        def c():
            return self._stop_queue.empty() or \
               not self._stop_queue.get(True, .1) == True

        def s():
            self.log.debug("alerting caller that we are all done")
            self._stopped_queue.put(True)

        self.log.debug("starting...")
        yield c
        self.log.debug("stopping...")
        s()

    def run(self):
        with self.context() as should_continue:
            self.work(should_continue)

    def work(self, should_continue):
        """
        subclasses should this implement this

        should_continue is a callable to return True/False instructing
        to continue working or not

        while should_continue():
            do_work()
        else:
            cleanup()
        """
        while should_continue():
            self.log.debug("running...")
        else:
            self.log.debug("cleaning up")

    def terminate(self):
        try:
            if self.is_alive():
                self._stop_queue.put(True)
                self.log.debug("terminating...")
                while not self._stop_queue.empty() \
                          and self.is_alive() \
                          and self._stopped_queue.empty():
                    self.log.debug("waiting for process to finish...")
                    time.sleep(.01)
                else:
                    self.log.debug("process got the message and replied ... or is no longer alive")
            else:
                pass
        finally:
            Process.terminate(self)
