timeout = 120
workers = 1
worker_class = "sync"
bind = "0.0.0.0:10000"

def post_fork(server, worker):
    from app import start_scheduler
    start_scheduler()
