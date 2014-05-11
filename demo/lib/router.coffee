Router.map ->
  @route 'landingPage',
    layoutTemplate: 'layout'
    path: '/'

  @route 'post',
    path: '/post'
    where: 'server'
    action: ()->
      console.log @request.body
      key = @request.body.key
      value = @request.body.value
      reqid = @request.body.reqid

      key =
        key: key
        value: value
        reqid: reqid
      Meteor.call 'makeKey', key, (error, id)->
        if error
          Errors.throw(error.reason)

      @response.writeHead(200, {'Content-Type': 'text/html'});
      @response.end('hello from server');
