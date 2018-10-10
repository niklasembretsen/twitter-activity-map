import express from 'express';

const server = express();

// Listen to /(startpage)
server.get('/', (req, res) => {

  res.send(template({
    body: '<p>hej</p>',
    title: 'Hello World from the server'
  }));
});

server.listen(3000);
console.log('listening');
