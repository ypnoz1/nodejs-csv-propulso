const express = require('express');
const metricRoute = express.Router();
const {
    readLargeFile,
    statsFile
} = require('../controller/metricController.js');

metricRoute.get('/main', readLargeFile);
metricRoute.get('/stats', statsFile);


module.exports = metricRoute; 