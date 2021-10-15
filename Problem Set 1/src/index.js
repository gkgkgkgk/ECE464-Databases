const { Sequelize, Op, Model, DataTypes } = require("sequelize");

// implement sequelize.js for mysql
const sequelize = new Sequelize("mysql://root:gkgkgkgk@localhost:3306/class", {
    define: {
        timestamps: false
    }
});

// confirm connection to database
sequelize.authenticate().then(() => {
    console.log("Connection has been established successfully.");
    query1();
}).catch(err => {
    console.error("Unable to connect to the database:", err);
});


class Sailor extends Model { }
Sailor.init({
    sid: { type: DataTypes.INTEGER, primaryKey: true },
    sname: DataTypes.STRING,
    rating: DataTypes.INTEGER,
    age: DataTypes.INTEGER
}, { sequelize, modelName: "sailor" });

class Boat extends Model { }
Boat.init({
    bid: { type: DataTypes.INTEGER, primaryKey: true },
    bname: DataTypes.STRING,
    color: DataTypes.STRING,
    length: DataTypes.INTEGER
}, { sequelize, modelName: "boat" });



class Reserve extends Model { }
Reserve.init({
    sid: { type: DataTypes.INTEGER },
    bid: DataTypes.INTEGER,
    day: DataTypes.DATE,
}, { sequelize, modelName: "reserve" });
// remove id attribute from reserve
Reserve.removeAttribute('id');


const query1 = () => {
    // List, for every boat, the number of times it has been reserved, excluding those boats that have never been reserved (list the id and the name).
    Boat.findAll({
        attributes: ['bid', 'bname'],
        include: [{
            model: Reserve,
            attributes: ['bid'],
            required: false,
        }],
        group: ['bid', 'bname'],
        having: {
            [Op.count]: 'bid'
        }
    }).then(boats => {
        console.log(boats);
    });
}

const query6 = () => {
    Sailor.findAll({
        where: {
            rating: 10
        },
        attributes: [Sequelize.fn('AVG', Sequelize.col('age'))],
        raw: true
    }).then(result => {
        console.log(result);
    });
}

