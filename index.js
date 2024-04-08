const express = require('express');
const allRoutes = require('./src/route/allRoutes.js');
const app = express();

allRoutes(app);

app.listen(4000); 