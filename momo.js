
load('underscore.js')
load('schema.js')


mongo_hacker_config = {
  verbose_shell:  true,             // additional verbosity
  index_paranoia: false,            // querytime explain
  enhance_api:    true,             // additonal api extensions
  indent:         2,                // number of spaces for indent
  sort_keys:      false,            // sort the keys in documents when displayed
  uuid_type:      'default',        // 'java', 'c#', 'python' or 'default'
  windows_warning: true,            // show warning banner for windows
  force_color:     false,           // force color highlighting for Windows users
  column_separator:  '→',           // separator used when printing padded/aligned columns
  value_separator:   '/',           // separator used when merging padded/aligned values

  // Shell Color Settings
  // Colors available: red, green, yellow, blue, magenta, cyan
  colors: {
    'key':       { color: 'gray' },
    'number':    { color: 'red' },
    'boolean':   { color: 'blue', bright: true },
    'null':      { color: 'red', bright: true },
    'undefined': { color: 'magenta', bright: true },
    'objectid':  { color: 'yellow', underline: true },
    'string':    { color: 'green' },
    'binData':   { color: 'green', bright: true },
    'function':  { color: 'magenta' },
    'date':      { color: 'blue' },
    'uuid':      { color: 'cyan' },
    'databaseNames':   { color: 'green', bright: true },
    'collectionNames': { color: 'blue',  bright: true }
  }
}

load('hacks.js')

DB.prototype.showCounts = function (){ 
  var self = this;
  var colls = self.getCollectionNames();
  var total=0; 
  var counts=[];
  colls.forEach(function(col){
    var count=self[col].count(); 
    counts.push({collection : col, count : count});
    total+=count 
  });
  showTable(counts);
  print('Total : ' + total) 
}

DB.prototype.getSlowOps = function(ms){
  if (ms === undefined) ms = 600;
  var ops = this.currentOp().inprog;
  var opids = [];
  ops.forEach(function(op){ 
    if (op.secs_running > ms  && op.ns.length > 0) { 
      opids.push({id : op.opid, ns : op.ns}) 
    }
  })
  return opids;
}

DB.prototype.killSlowOps = function(ms){
  var opids = getSlowOps(ms);
  var self = this;
  opids.forEach(function(op){ 
    print('KILLING ' + op.id)
    self.killOp(op.id) 
  })
}

DB.prototype.selectCollections = function(re){
  var cns = this.getCollectionNames();
  if (!re) return cns;
  if (_.isString(re)) re = new RegExp(re);
  var cols = [];
  cns.forEach(function(n){
    if (re.test(n)) cols.push(n)
  })
  return cols;
}
DB.prototype.dropCollections = function(re){
  var cols = _.isArray(re) ? re : this.selectCollections(re);
  cols.forEach(function(c){
    print(colorize('Dropping ' + c, {color: 'red'}))
    db[c].drop();
  })
}
DBQuery.prototype.reduce = function(callback, initialValue) {
  var result = initialValue;
  this.forEach(function(obj){
    result = callback(result, obj);
  });
  return result;
}

DBQuery.prototype.showTable = function(fields, strLimit){
    try {
        var rs = [];
        var n = 0;
        while ( this.hasNext() && n < DBQuery.shellBatchSize ){
            rs.push(this.next())
            n++;
        }
        
        showTable(rs, fields, strLimit)
    }
    catch ( e ){
        print( e );
    }
}


DBCollection.prototype.showIndex = function(){
    try {
        var rs = this.getIndexes().map(function(i){return {name : i.name, key : JSON.stringify(i.key)}})
        
        showTable(rs)
    }
    catch ( e ){
        print( e );
    }
}

DBQuery.prototype.showSchema = function(fields, strLimit){
    try {
        if (!this.hasNext()) {
          return print('Could not get the schema of an empty result set!')
        }
        var rs = [];
        var n = 0;
        while ( this.hasNext() && n < DBQuery.shellBatchSize ){
            rs.push(this.next())
            n++;
        }
        
        showSchema(rs)
    }
    catch ( e ){
        print( e );
    }
}


DBCollection.prototype.showSchema = function(){
    this.find().reverse().showSchema();
}

prompt = function() {
	return db+" >";
}

function getDbs(){
	return db.adminCommand('listDatabases');
}

function use(dbname){
	db = db.getSiblingDB(dbname);
}

function getTable(results, fields, strLimit) {

  if (fields === undefined) {
  	fields = _.keys(results[0])
  }
  var headers = fields;
  if (strLimit === undefined) strLimit = 80;
  var rows = results.map(function(row) {
    return fields.map(function(f){
      var parts = f.split('.')
      var v = row;
      for(var i = 0; i < parts.length; i++){
        if (v) v = v[parts[i]];
      }
      return v
    });
  });
  var table = [headers, headers.map(function(){return '';})].concat(rows);
  var limitString = function(str) { return _.isString(str) ? (str.length > strLimit ? str.substring(0, strLimit) + '...' : str) : str + "" }
  var colMaxWidth = function(arr, index) {return Math.max.apply(null, arr.map(function(row){return limitString(row[index]).length;}));};
  var pad = function(width, string, symbol) { return width <= string.length ? string : pad(width, isNaN(string) ? string + symbol : symbol + string, symbol); };
  table = table.map(function(row, ri){
    return '| ' + row.map(function(cell, i) {return pad(colMaxWidth(table, i), limitString(cell), ri === 1 ? '-' : ' ');}).join(' | ') + ' |';
  });
  var border = '+' + pad(table[0].length - 2, '', '-') + '+';
  return [border].concat(table).concat(border).join('\n');
}

function showTable(results, fields, strLimit){
  print(getTable(results, fields, strLimit))
}

function showSchema(items, maxDepth){
  if (!_.isArray(items)){
    return print(items + " is not an array!")
  }
  var schema = getSchema(items, maxDepth);
  var results = schema.map(function(item){return {
    fields : item._id.key,
    occurrences : item.totalOccurrences,
    '' : item.percentContaining + "%"
  }})
  showTable(results)
}

function getIsoWeek(dt) {
  var target  = new Date(dt.valueOf());
  var dayNr   = (dt.getUTCDay() + 6) % 7;
  target.setUTCDate(target.getUTCDate() - dayNr + 3);
  var firstThursday = target.valueOf();
  target.setUTCMonth(0, 1);
  if (target.getUTCDay() != 4) {
    target.setUTCMonth(0, 1 + ((4 - target.getUTCDay()) + 7) % 7);
  }
  return 1 + Math.ceil((firstThursday - target) / 604800000); 
}
function getIsoWeekYear(dt) {
  var target  = new Date(dt.valueOf());
  target.setUTCDate(target.getUTCDate() - ((dt.getUTCDay() + 6) % 7) + 3);  
  return target.getUTCFullYear();
}
function getIsoFullWeek(dt){
  return getIsoWeekYear(dt) * 100 + getIsoWeek(dt);
}
function getIsoFullDay(dt){
  return (dt.getUTCFullYear() * 100 + (1 + dt.getUTCMonth())) * 100 + dt.getUTCDate();
}
function getIsoFullMonth(dt){
  return dt.getUTCFullYear() * 100 + (1 + dt.getUTCMonth());
}


