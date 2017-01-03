'use strict';


// https://github.com/arjunmehta/node-georedis

const url            = require('url');
const redis          = require('redis');
const getRedisClient = () => {
  const redisUrl  = url.parse('redis://localhost:6379');
  const client    = redis.createClient(redisUrl.port, redisUrl.hostname);
  // console.log({ action: 'app', redisUrl: redisUrl });
  if (redisUrl.auth) {
    client.auth(redisUrl.auth.split(":")[1]);
  }
  return client;
}

const client = getRedisClient();
const geo = require('georedis').initialize(client);

const doOnce = () => {
  const N = 100000;
  const center    = [-73.993549, 40.727248];
  const lowerLeft = [-74.009180, 40.716425];
  const deltaLon  = Math.abs(lowerLeft[0] - (-73.97725));
  const deltaLat  = Math.abs(lowerLeft[1] - (40.7518692));
  let   tPrevious = 1475431264754;

  let locationSet = {};
  let timeSet     = ['incident_times'];

  for (let i = 0; i < N; i++) {
    const incidentLon = lowerLeft[0] + Math.random() * deltaLon;
    const incidentLat = lowerLeft[1] + Math.random() * deltaLat;
    tPrevious        += Math.random() * 60 * 1000; // random time after previous
    const ll          = { latitude: incidentLat, longitude: incidentLon };
    const key         = '-k'+i;
    locationSet[key]  = ll;
    // time
    const ts          = tPrevious;
    timeSet.push(ts);
    timeSet.push(key);

    client.hmset(key, { latitude: incidentLat, longitude: incidentLon, ts: ts });
  }    

/*
  client.zadd(timeSet, (err,res) => {
    if (err) {
      console.error(err);
    }
    else {
      console.log('added times:',res);
    }  
  })

  geo.addLocations(locationSet, (err,res) => {
    if (err) {
      console.error(err);
    }
    else {
      console.log('added locations:',res);
    }
  })  
  */
}

const nearby = (options) => {
  return new Promise( (resolve,reject) => {
    geo.nearby({latitude: options.latitude, longitude: options.longitude}, options.meters, (err, locations) => {
      if (err) {
        reject(err);
      }
      else {
        resolve(locations);
      }
    })
  });
}

const get = (key) => {
  return new Promise( (resolve,reject) => {
    client.hgetall(key, (err,res) => {
      if (err) {
        reject(err);
      }
      else {
        resolve(res);
      }
    });
  });
}

const t0 = Date.now();
nearby({ latitude: 40.727248, longitude: -73.993549, meters: 500 }).then( (locations) => {
  let aPromises = [];
  for (let i in locations) {
    const key = locations[i];
    aPromises.push(get(key));
  }
  Promise.all(aPromises).then( (aResults) => {
    for (let i in aResults) {
      aResults[i].key = locations[i];
    }
    let aSorted = aResults.sort( (a,b) => b.ts - a.ts ).slice(0,50)
    console.log({ action: 'sorted', dT: Date.now() - t0, aSorted:aSorted }); 
  })
  console.log({ action: 'nearby', dT: Date.now() - t0 }); // seemed slow 60something ms for small radius, 200-300ms for 5k radius
})
.catch( (err) => {
  console.error(err);
})



