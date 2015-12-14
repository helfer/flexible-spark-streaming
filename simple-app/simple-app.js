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

    var chart;
    var series;
    var seriesMap = new Object();
    var seriesMapLastIndex = 0;

    Template.interact.helpers({
        currently_running: function() {
            return Queries.find();
        }
    });

    Template.interact.events({
      'click .cancel_query': function(event,template){
        console.log("1");
        console.log(seriesMap);
        console.log(seriesMapLastIndex);
        var id = event.target.id;

        Queries.remove({_id: id});

        var i = seriesMap[id];
        delete seriesMap[id];

        chart.series[i].remove();
        seriesMapLastIndex -= 1;

        for(var j = 0; j < chart.series.length; j++) {
            var thisID = chart.series[j].name;
            console.log("thisID: " + thisID);
            seriesMap[thisID] = j;
        }

        console.log("2");
        console.log("series: ");
        console.log(chart.series);
        console.log(seriesMap);
        console.log(seriesMapLastIndex);
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

          Meteor.subscribe('results', query_id );

          seriesMapLastIndex += 1;
          seriesMap[query_id] = seriesMapLastIndex;

            var thisSeries = {
                name: query_id,
                data: []
            }
            
            chart.addSeries(thisSeries, true);
            
          // all active queries should be listed somewhere
          // (later it should be possible to remove queries ...)
        },

        'click .reset': function(){
          Meteor.call('reset');
        }
    });

    // Set an observer to be triggered when Results.insert() is invoked
    Results.find().observe({
        'added': function(item) {
            var x = (new Date()).getTime(); // current time
            var y = item.values[0];

            var s = item.query_id;
            var index = seriesMap[s];

            chart.series[index].addPoint({
                x: x,
                y: y,
                cmd: item.query_id
            }, true);
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
                name: 'Initial Series',
                data: []
            }]
        });

        chart = $('#container-graph').highcharts();
        series = chart.series[0];

        Meteor.subscribe('queries', function() {// callback upon completion
            var allQueries = Queries.find().fetch();
            seriesMapLastIndex = allQueries.length - 1;

            for (var i = 0; i < allQueries.length; i++) {
                seriesMap[allQueries[i]._id] = i;

                Meteor.subscribe('results', allQueries[i]._id );

                var thisSeries = {
                    name: allQueries[i]._id,
                    data: []
                }
                if (i > 0) {
                    chart.addSeries(thisSeries, false);
                }
            }

            chart.redraw();
        });

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
        'reset': function(){
          Queries.remove({});
          Results.remove({});
        }
    });
}
