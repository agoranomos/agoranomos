"""The services that the stack tests expect in local mode."""

LONG_RUNNING = {"warehouse-db", "platform-db"}
ONE_SHOT = {"db-init"}
