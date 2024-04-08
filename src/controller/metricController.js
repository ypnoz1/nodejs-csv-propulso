const fs = require('fs');
const {getDistanceBetweenPoints, 
    isValidTimeStamp, 
    isValidDelta, 
    isNewVisit
} = require('../utils/tools');  
const { TIME_STAMP_TO_DAYS,NB_MONTHS } = require('../utils/constant');  

const readLargeFile = ((req, res) => {
    const pageFrom = req.query.start ?? 0;
    const pageTo   = req.query.end ?? 200;
    const pageStep = pageTo - pageFrom;

    const file = `src/files/dataset_pathing_expanded.csv`;
    const readStream = fs.createReadStream(file);

    let count = 0;
    let sumOfCsvSize = 0;
    let prevSumCsvSize = 0;
    let chunkPartOne = null;
    let chunkPartTwo = null;
    let chunkOverLap = false;
    let ajustPageStart = 0;
    let remainderFromPrev = 0;
    readStream.on('data', (chunk) => {
        const csvTab  = chunk.toString().split('\n');
        const sizeTab = csvTab.length;

        prevSumCsvSize = sumOfCsvSize;
        sumOfCsvSize  += sizeTab;
        chunkOverLap   = (pageFrom < sumOfCsvSize && pageTo > sumOfCsvSize) ? true : chunkOverLap;

        if(chunkOverLap || sumOfCsvSize >= pageTo){
            chunkPartOne = chunkPartTwo;
            chunkPartTwo = chunk;

            if(sumOfCsvSize >= pageTo){
                remainderFromPrev = prevSumCsvSize % 100;
                ajustPageStart = prevSumCsvSize - remainderFromPrev;
                readStream.close();
            }
        }
        count++;
    });
    readStream.on('close', (chunk) => {
        let csvTab = chunkPartTwo.toString().split('\n');
        let overLapTab = [];        
        if(chunkOverLap){
            overLapTab = chunkPartOne.toString().split('\n');
            overLapTab = overLapTab.slice(overLapTab.length - remainderFromPrev);
            csvTab = [...overLapTab, ...csvTab];
        }
        const startSaving   = parseInt(pageFrom) - ajustPageStart;
        const limitOfSaving = startSaving + parseInt(pageStep);
        
        const saveResult = [];
        let counter = 0;
        for(const prop in csvTab){
            if(counter >= startSaving && counter <= limitOfSaving){
                const dataLine = csvTab[prop].split(',');
                let [id, lat, lon, delta, unix_timestamp] = dataLine;
                saveResult.push({
                    id,
                    lat,
                    lon,
                    delta_time:delta,
                    timestamp: unix_timestamp
                });
            }
            if(counter === limitOfSaving){
                break;
            }
            counter++;
        }
        res.end(JSON.stringify(saveResult));
        readStream.destroy();
    });
    readStream.on('end', (chunk) => {
        readStream.close();
        console.log("All rows successfully processed"); 
    });
});

const statsFile = ((req, res) => {
    const fileStream = `src/files/dataset_pathing_expanded.csv`;
    //const fileStream = `src/files/dataset_pathing_expanded_short.csv`;
    const readStream = fs.createReadStream(fileStream);
    const sizeStream = fs.statSync(fileStream).size;

    //pointers vars
    let pointerPrev = {id:'', lat:'', lon:'', delta:'', time:''};
    let pointerNow  = { ...pointerPrev };//current user
    let prevVisitor = { ...pointerPrev };//prev first occurence of current user
    let currVisitor = { ...pointerPrev };//current user

    let statsVisits = {};//save data
    let saveInBetweenVisits = {};
    
    let chunkSize   = 0;
    let entriesCount = 0;

    readStream.on('data', (chunk) => {
        chunkSize += chunk.toString().length;
        const csvTab  = chunk.toString().split('\n');
        entriesCount += csvTab.length;
        for(const prop in csvTab){
            const dataLine = csvTab[prop].split(',');
            let [id, lat, lon, delta, unix_timestamp] = dataLine;
            
            if(id.length > 1 && !Number.isNaN(unix_timestamp) && String(unix_timestamp).length >= 10){//valid date
                pointerPrev = pointerNow;
                pointerNow  = { id, lat, lon, delta, unix_timestamp };

                const dateVisit = new Date(unix_timestamp * 1000);
                const month = dateVisit.getMonth() + 1;

                if(!Object.hasOwn(statsVisits, month)){
                    statsVisits[month] = {
                        visits: 0,
                        visitors: {},
                        speed_average: 0,
                        distance_total: 0,
                        duration: {
                            count: 0,
                            total: 0,
                            average: 0
                        },
                        nb_days_no_visit: {
                            count: 0,
                            nb_days: 0
                        }                  
                    };
                }

                if(currVisitor.id !== id && isValidDelta(delta)){
                    /**
                     *  2 lines of the same user.
                     *  prevVisitor 1st occurence of user.
                     *  pointerPrev last occurence of user.
                    **/
                    prevVisitor = currVisitor;
                    currVisitor = { id, lat, lon, delta, unix_timestamp };

                    if(isNewVisit(currVisitor.id, prevVisitor.id)){
                        statsVisits[month].visits += 1;
                        if(!Object.hasOwn(statsVisits[month].visitors, currVisitor.id)){
                            statsVisits[month].visitors[currVisitor.id] = 1;
                        }

                        //range from time [first] to last time [last]
                        let numberTimeStampStart = parseInt(prevVisitor.unix_timestamp);
                        let numberTimeStampEnd   = parseInt(pointerPrev.unix_timestamp);
                        if(isValidTimeStamp(numberTimeStampStart) && isValidTimeStamp(numberTimeStampEnd)){     
                            //duration in secs
                            statsVisits[month].duration.count  += 1;
                            statsVisits[month].duration.total  += Math.abs(numberTimeStampStart - numberTimeStampEnd);
                            statsVisits[month].duration.average = parseInt(statsVisits[month].duration.total / statsVisits[month].duration.count);
                            
                            //distance in meter
                            const distance = getDistanceBetweenPoints(prevVisitor.lat, prevVisitor.lon, pointerPrev.lat, pointerPrev.lon, 'meters');
                            if(!Number.isNaN(distance) && distance > 0){
                                statsVisits[month].distance_total += distance;
    
                                //speed meter/seconds
                                statsVisits[month].speed_average = statsVisits[month].distance_total / statsVisits[month].duration.total;
                            }
                        }
                        
                        if(!Object.hasOwn(saveInBetweenVisits, pointerPrev.id)){//last occurence of visit
                            saveInBetweenVisits[pointerPrev.id] = { id: pointerPrev.id, unix_timestamp: pointerPrev.unix_timestamp};
                        }else
                        if(Object.hasOwn(saveInBetweenVisits, currVisitor.id) && saveInBetweenVisits[currVisitor.id].id === currVisitor.id){//first time since last visit
                            numberTimeStampStart = parseInt(saveInBetweenVisits[currVisitor.id].unix_timestamp);
                            numberTimeStampEnd   = parseInt(currVisitor.unix_timestamp);
                            if(isValidTimeStamp(numberTimeStampStart) && isValidTimeStamp(numberTimeStampEnd)){
                                const daysDiff = Math.abs((numberTimeStampStart - numberTimeStampEnd) / TIME_STAMP_TO_DAYS);
                                if(daysDiff > 0){//day length in time
                                    statsVisits[month].nb_days_no_visit.count   += 1;
                                    statsVisits[month].nb_days_no_visit.nb_days += daysDiff;
                                }
                                delete saveInBetweenVisits[currVisitor.id];
                            }
                        }
                    }
                }
            }
        }
        const loadingJson = {
            sizeStream,
            entriesCount,
            chunkSize
        };
        res.write(JSON.stringify(loadingJson));
    });

    readStream.on('end', (chunk) => {
        let monthlyAvgVisit       = 0;
        let monthlyAvgVisitor     = 0;
        let visitAverageDuration  = 0;
        let visitAverageSpeed     = 0;
        let noVisitDayAverage     = 0;
        let countNoVisitDay       = 0;
        let monthlyDurationRecord = [];
        let monthlySpeedRecord    = [];
        let monthlyVisitRecord    = [];
        let monthlyVisitorRecord  = [];

        Object.keys(statsVisits).forEach(monthKey => {
            const visitorsMonthly = Object.keys(statsVisits[monthKey].visitors).length;
            const noVisit = statsVisits[monthKey].nb_days_no_visit.nb_days / statsVisits[monthKey].nb_days_no_visit.count

            monthlyAvgVisit      += statsVisits[monthKey].visits;
            monthlyAvgVisitor    += visitorsMonthly;
            visitAverageDuration += statsVisits[monthKey].duration.average;
            visitAverageSpeed    += statsVisits[monthKey].speed_average;
            noVisitDayAverage    += noVisit;

            const secsToMin       = statsVisits[monthKey].duration.average / 60;
            monthlyDurationRecord.push(secsToMin);
            monthlySpeedRecord.push(statsVisits[monthKey].speed_average);
            monthlyVisitRecord.push(statsVisits[monthKey].visits);
            monthlyVisitorRecord.push(visitorsMonthly);
        });

        monthlyAvgVisit      = parseInt(monthlyAvgVisit / NB_MONTHS);
        monthlyAvgVisitor    = parseInt(monthlyAvgVisitor / NB_MONTHS);
        visitAverageDuration = parseInt(visitAverageDuration / NB_MONTHS);
        visitAverageSpeed    = visitAverageSpeed / NB_MONTHS;
        noVisitDayAverage    = parseInt(noVisitDayAverage / NB_MONTHS);

        const stats = {
            monthlyAvgVisit,
            monthlyAvgVisitor,
            visitAverageDuration,
            visitAverageSpeed,
            noVisitDayAverage,
            monthlyDurationRecord,
            monthlySpeedRecord,
            monthlyVisitRecord,
            monthlyVisitorRecord
        }
        res.end(JSON.stringify(stats));
        readStream.close();

        console.log("All rows successfully processed"); 
    });
});

module.exports = {
    readLargeFile,
    statsFile
}