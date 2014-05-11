@Keys = new Meteor.Collection 'keys'

Meteor.methods(
  makeKey: (keyAttributes)->
    keyId = Keys.insert(keyAttributes)
    keyId
)
