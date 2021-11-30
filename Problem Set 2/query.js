const { MongoClient } = require('mongodb');

const uri = "mongodb+srv://gkgkgkgk:gkgkgkgk@cluster0.q4sdx.mongodb.net/Cluster0?retryWrites=true&w=majority";
const client = new MongoClient(uri, { useNewUrlParser: true, useUnifiedTopology: true });
client.connect(async (err) => {
    if (err) {
        console.log(err);
    }
    console.log("Connected to MongoDB");

    const db = client.db("allkicks");
    const collection = db.collection("kicks");

    // find US based kickstarters with a goal of more than 1000000 USD
    collection.find({ goal: { $gt: 1000000 }, country: "US" }).toArray((err, result) => {
        // console.log(result);
    });

    // count kickstarters that made more than 10,000,000 USD
    let n = await collection.find({ usd_pledged_real: { $gt: 10000000 } }).count();
    console.log(n);
    // find kickstarters that that made more than $100000 in the film and video category
    n = await collection.find({ usd_pledged_real: { $gt: 100000 }, main_category: "Film & Video" }).toArray();
    console.log(n);

    // find amount of failed kickstarters that made over 100,000 USD
    n = await collection.find({ usd_pledged_real: { $gt: 100000 }, state: "failed" }).count();
    console.log(n);

    // find kickstarters that succeeeded with less than 100 backers
    n = await collection.find({ state: "successful", backers: { $lt: 100 } }).count();
    console.log(n);

    // find kickstarter with highest pledged amount
    n = await collection.find({}).sort({ usd_pledged_real: -1 }).limit(1).toArray();
    console.log(n);

    // find first successful kickstarter
    n = await collection.find({ state: "successful" }).sort({ launched: 1 }).limit(1).toArray();
    console.log(n);

    // find kickstarter with most backers (interestingly, not even close to the most amount pledged)
    n = await collection.find({}).sort({ backers: -1 }).limit(1).toArray();
    console.log(n);
});