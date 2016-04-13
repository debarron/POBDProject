var Subjects = require('./models/SubjectViews');

//app.get('/api/data', function (req, res) {
module.exports = function (app) {
    app.get('/api/data', function (req, res) {
        
        // use mongoose to get all nerds in the database
        Subjects.find({}, {}, function (err, subjectDetails) {
            
            res.json(subjectDetails); // return all nerds in JSON format

        });

        
    });
}