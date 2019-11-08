# Mysql Docker with backups to AWS S3

## Installing/Using
1. Create a full backup
2. Run `FLUSH LOGS` and `PURGE BINARY LOGS BEFORE NOW()` on server
3. Update configuration in backup.py
4. Setup a cron job to run periodically
