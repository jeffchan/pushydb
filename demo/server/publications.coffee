Meteor.publish 'keys', ()->
  return Keys.find({})

Meteor.startup( ()->
  Keys.remove({})
)
