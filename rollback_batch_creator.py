from common import ROLLBACK, create_batches
if __name__ == "__main__":
    create_batches(ROLLBACK)       # run every minute from cron (only needed during rollback)
