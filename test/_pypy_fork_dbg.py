import os
import threading

from pymongo import MongoClient


def test_many_threaded():
    # Fork randomly while doing operations.
    class ForkThread(threading.Thread):
        def __init__(self):
            super().__init__()

        def print(self, msg):
            print(f"{os.getpid()}-{self.ident}: {msg}")

        def run(self) -> None:
            clients = []
            for _ in range(10):
                clients.append(MongoClient())

            # The sequence of actions should be somewhat reproducible.
            # If truly random, there is a chance we never actually fork.
            # The scheduling is somewhat random, so rely upon that.
            def action(client):
                print("ping")
                client.admin.command("ping")
                return 0

            for i in range(200):
                # Pick a random client.
                rc = clients[i % len(clients)]
                if i % 50 == 0:
                    # Fork
                    pid = os.fork()
                    if pid == 0:  # Child => Can we use it?
                        code = -1
                        try:
                            code = action(rc)
                        finally:
                            os._exit(code)
                    else:  # Parent => Child work?
                        print(f"waitpid: {os.waitpid(pid, 0)[1]}")

                action(rc)

            for c in clients:
                pass
            self.print("Done")

    threads = [ForkThread() for _ in range(10)]
    for t in threads:
        t.start()

    for t in threads:
        t.join()


test_many_threaded()
