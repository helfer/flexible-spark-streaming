import os
import threading
import time

import safethread

class DirWatcher(safethread.SafeThread):

    def __init__(self, dirname, callback, interval=1):
        super(DirWatcher, self).__init__(name="DirWatcher")
        self.dirname = dirname
        self.callback = callback
        self.interval = interval
        self.old_files = set()

    def action(self):
        cur_files = set(os.listdir(self.dirname))
        if cur_files != self.old_files:
            changes = {
                'added': list(cur_files - self.old_files),
                'removed': list(self.old_files - cur_files)
            }
            self.callback(changes)
            self.old_files = cur_files
        time.sleep(self.interval)
