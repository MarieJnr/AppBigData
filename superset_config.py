import os
# Superset specific config
ROW_LIMIT = 5000

# Flask App Builder configuration
# Your App secret key will be used for securely signing the session cookie
# and encrypting sensitive information on the database
# Make sure you are changing this key for your deployment with a strong key.
# Alternatively you can set it with `SUPERSET_SECRET_KEY` environment variable.
# You MUST set this for production environments or the server will refuse
# to start and you will see an error in the logs accordingly.
SECRET_KEY = 'xZ6pyBACwF0FlVEWXO8QUKWjU3yGBi+steBzVjOVeqacDqMlmdvwFePv'
