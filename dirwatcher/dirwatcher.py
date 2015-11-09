import os, time
from threading import Thread


class DirWatcher():

    def __init__(self, dirname, cb, timeout = 1):
        self.dirname = dirname
        self.cb = cb
        self.timeout = timeout
        self.stopped = True


    def run(self):
        old_files = set()
        while not self.stopped:
            cur_files = set(os.listdir(self.dirname))
            if cur_files != old_files:
                changes = {
                    'added': list(cur_files - old_files),
                    'removed': list(old_files - cur_files)
                }
                self.cb(changes)
                old_files = cur_files
            time.sleep(self.timeout)


    def start(self):
        self.stopped = False
        thread = Thread(target=self.run, args=())
        thread.start()


    def stop(self):
        self.stopped = True

