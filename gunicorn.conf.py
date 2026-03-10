timeout          = 120
workers          = 1
worker_class     = "sync"
threads          = 4
bind             = "0.0.0.0:10000"
max_requests     = 1000
max_requests_jitter = 50
preload_app      = False

def post_fork(server, worker):
    from app import start_scheduler
    start_scheduler()
