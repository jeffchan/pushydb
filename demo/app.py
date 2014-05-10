from flask import Flask
app = Flask(__name__)

@app.route('/')
def index():
  return "6.824 final project"

@app.route('/push', methods=['POST'])
def push():
  print "got a push"

if __name__ == '__main__':
  app.run()
