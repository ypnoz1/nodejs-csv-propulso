const getDistanceBetweenPoints = (latitude1, longitude1, latitude2, longitude2, unit = 'miles') => {
    if(!Number.isNaN(latitude1) && !Number.isNaN(longitude1) && !Number.isNaN(latitude2) && !Number.isNaN(longitude2)){
        let theta = longitude1 - longitude2;
        let distance = 60 * 1.1515 * (180/Math.PI) * Math.acos(
            Math.sin(latitude1 * (Math.PI/180)) * Math.sin(latitude2 * (Math.PI/180)) + 
            Math.cos(latitude1 * (Math.PI/180)) * Math.cos(latitude2 * (Math.PI/180)) * Math.cos(theta * (Math.PI/180))
        );
        if(unit == 'miles'){
            return Math.round(distance, 2);
        }else if(unit == 'kilometers'){
            return Math.round(distance * 1.609344, 2);
        }else if(unit == 'meters'){
            return Math.round(distance * 1.609344 * 1_000, 2);
        }
    }
    return 0;
}
var isValidTimeStamp = (timestamp) => {
    return String(timestamp).length >= 10 && (new Date(timestamp)).getTime() > 0;
}
const isNewVisit = (currId, prevId) => {
    return currId !== prevId && prevId.length > 1;
}
const isValidDelta = (delta) => {
    return delta <= 0;
}
module.exports = { 
    getDistanceBetweenPoints, 
    isValidTimeStamp,
    isNewVisit,
    isValidDelta
}; 