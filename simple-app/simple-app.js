var Queries = new Meteor.Collection('queries');
/* Format for queries collection:
 *
 * {
 *  _id: query id, automatically assigned by mongodb
 *  select: { agg: 'max', field: 'retweets' }, // select is just one field for now. can be array later maybe.
 *  from: { start: 0, end: 0}, // we don't do range queries yet, so this field is ignored
 *  where: { _and: [ { text: { _contains: 'abc' } }, { lang: { _eq: 'en' } } ] } // _and and _or can be nested
 *  // we only support _contains and _eq (equals) for now
 *  group_by: 0 //let's not do this for now, we can implement it later.
 *
 */
var Results = new Meteor.Collection('results');
/* Format for Results collection
 *
 * {
 *  _id: xyz, //we don't care about this
 *  query_id: x, //this references the query
 *  time: Date(), // a date object or other timestamp so we can sort on it
 *  values: [ 5 ], // an array with just one value for now. maybe more later.
 *  // when we support group_by, values will have to become an object.
 *
 */


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
    //Session.setDefault('counter', 0);

    Template.interact.helpers({
        currently_running: function() {
            return Queries.find();
        }
    });

    Template.interact.events({
      'click .cancel_query': function(event,template){
        Queries.remove({_id: event.target.id});
      }
    });

    // Add an event listener for Run-button
    Template.interact.events({
        'submit #queryform': function( event, template ){
          event.preventDefault();
          console.log(event.target.query_from.value);
          // the query form should be expanded to contain separate input fields or selects for each argument.

          // dummy query:
          var query_id = Queries.insert({
              select: { agg: event.target.query_select_aggregator.value , field: event.target.query_select_field.value },
              from: 0, //ignored
              where: {text:{ _contains:event.target.query_where_value.value} }
          });
          console.log('inserted ' + query_id);
          Meteor.subscribe('results', query_id );

          // all active queries should be listed somewhere
          // (later it should be possible to remove queries ...)
        },

        'click .reset': function(){
          Meteor.call('reset');
        }
    });

    // Start listening changes in Replies
    Meteor.autosubscribe(function() {
        Meteor.subscribe('queries');
        Meteor.subscribe('results');
    });

    // Set an observer to be triggered when Results.insert() is invoked
    Results.find().observe({
        'added': function(item) {
            //Session.set('', item.message);
            var x = (new Date()).getTime(), // current time
            y = item.values[0];

            series.addPoint({
                x: x,
                y: y,
                cmd: item.query_id
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

    Meteor.publish('queries', function() {
        return Queries.find();
    });

    Meteor.publish('results', function(query_id) {
        return Results.find( {query_id: query_id} );
    });

    Meteor.methods({
        /*
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
        },
        */
        'reset': function(){
          Queries.remove({});
        }
    });
}
