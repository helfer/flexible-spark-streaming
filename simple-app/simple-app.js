var Replies = new Meteor.Collection('replies');

if (Meteor.isClient) {

    var time = (new Date()).getTime();
    var initialData = [
        {x: time - 19000, y:0, cmd: 'top'},
        {x: time - 18000, y:0, cmd: 'ls'},
        {x: time - 17000, y:0, cmd: 'top'},
        {x: time - 16000, y:0, cmd: 'pwd'},
        {x: time - 15000, y:0, cmd: 'cd'},
        {x: time - 14000, y:0, cmd: 'vim'},
        {x: time - 13000, y:0, cmd: 'pwd'},
    ];
    var chart;
    var series;

    // counter starts at 0
    Session.setDefault('counter', 0);

    Template.interact.helpers({
        counter: function () {
            return Session.get('counter');
        },

        // Show the last command in input field
        last_cmd: function() {
            return Session.get('last_cmd');
        },

        // Show the last shell reply in browser
        window: function() {
            return Session.get('stdout');
        }
    });

    // Add an event listener for Run-button
    Template.interact.events({
        'click [type="button"]': function() {
            Session.set('counter', Session.get('counter') + 1);

            var cmd = $('#command').val();
            Session.set('last_cmd', cmd);

            // Call the command method in server side
            Meteor.call('command', cmd);
        }
    });

    // Start listening changes in Replies
    Meteor.autosubscribe(function() {
        Meteor.subscribe('replies');
    });

    // Set an observer to be triggered when Replies.insert() is invoked
    Replies.find().observe({
        'added': function(item) {
            // Set the terminal reply to Session
            Session.set('stdout', item.message);

            var x = (new Date()).getTime(), // current time
            y = Math.random();
            series.addPoint({
                x: x,
                y: y,
                cmd: item.cmd
            }, true, true);
        }
    });

    Meteor.startup(function() {

        Highcharts.setOptions({
            global: {
                useUTC: false
            }
        });

        $('#container-graph').highcharts({
            chart: {
                type: 'spline',
                animation: Highcharts.svg, // don't animate in old IE
                marginRight: 10

                /*
                events: {
                    load: function () {

                        // set up the updating of the chart each second
                        var series = this.series[0];
                        setInterval(function () {
                            var x = (new Date()).getTime(), // current time
                            y = Math.random();
                            series.addPoint([x, y], true, true);
                        }, 1000);
                    }
                }
                */
            },
            title: {
                text: null
            },
            xAxis: {
                type: 'datetime',
                tickPixelInterval: 150
            },
            yAxis: {
                title: {
                    text: 'Value'
                },
                plotLines: [{
                    value: 0,
                    width: 1,
                    color: '#808080'
                }]
            },
            tooltip: {
                formatter: function () {
                    return '<b>' + this.point.cmd + '</b><br/>' +
                    Highcharts.dateFormat('%Y-%m-%d %H:%M:%S', this.x) + '<br/>' +
                    Highcharts.numberFormat(this.y, 2);
                }
            },
            legend: {
                enabled: false
            },
            exporting: {
                enabled: false
            },
            series: [{
                name: 'Random data',
                data: initialData
            }]
        });

        chart = $('#container-graph').highcharts();
        series = chart.series[0];
    });

}

if (Meteor.isServer) {
    var exec;
    var Fiber;

    // Initialize the exec function
    Meteor.startup(function() {
        exec = Npm.require('child_process').exec;
        Fiber = Npm.require('fibers');
    });

    // Trigger the observer in Replies collection
    Meteor.publish('replies', function() {
        return Replies.find();
    });

    Meteor.methods({
        'command': function(line) {            
            // Run the requested command in shell
            exec(line, function(error, stdout, stderr) {
                // Collection commands must be executed within a Fiber
                Fiber(function() {
                    Replies.remove({});
                    Replies.insert({message: stdout ? stdout : stderr,
                                    cmd: line});
                }).run();
            });
        }
    });
}
