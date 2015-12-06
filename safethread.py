import thread
import threading
import traceback

class SafeThread(threading.Thread):

    def __init__(self, *args, **kwargs):
        super(SafeThread, self).__init__(*args, **kwargs)
        self.stop_flag = threading.Event()

    def action(self):
        raise NotImplementedError()

    def run(self, *args, **kwargs):
        try:
            while not self.stop_flag.is_set():
                self.action(*args, **kwargs)
        except:
            traceback.print_exc()
            # Fail fast by causing the entire program to exit
            thread.interrupt_main()

    def start(self):
        self.stop_flag.clear()
        super(SafeThread, self).start()

    def stop(self):
        self.stop_flag.set()
