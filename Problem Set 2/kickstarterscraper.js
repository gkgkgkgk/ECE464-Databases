const { MongoClient } = require('mongodb');
const csv = require('csvtojson');

const uri = "mongodb+srv://gkgkgkgk:gkgkgkgk@cluster0.q4sdx.mongodb.net/Cluster0?retryWrites=true&w=majority";
const client = new MongoClient(uri, { useNewUrlParser: true, useUnifiedTopology: true });
client.connect(async (err) => {
    if (err) {
        console.log(err);
    }
    console.log("Connected to MongoDB");
    const collection = client.db("allkicks").collection("kicks");

    // go through kcikstart.csv
    csv()
        .fromFile('./kickstarters.csv')
        .then((jsonObj) => {
            let i = 0;
            console.log(jsonObj.length);
            setInterval(() => {
                let j = jsonObj[i++];
                j.goal = parseFloat(j.goal);
                j.usd_goal_real = parseFloat(j.usd_goal_real);
                j['usd pledged'] = parseFloat(j['usd pledged']);
                j.usd_pledged_real = parseFloat(j.usd_pledged_real);
                j.backers = parseInt(j.backers);
                j.launched = new Date(j.launched);
                j.deadline = new Date(j.deadline);
                j.ID = parseInt(j.ID);
                j.pledged = parseFloat(j.pledged);
                collection.insertOne(j);
                console.log(j);
                if (i >= jsonObj.length) {
                    return;
                }
            }, 10);
        });
});