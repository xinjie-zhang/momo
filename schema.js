function getSchema(items, maxDepth){
  if (maxDepth === undefined) maxDepth = 99;
var varietyTypeOf = function(thing) {
  if (typeof thing === 'undefined') { throw 'varietyTypeOf() requires an argument'; }

  if (typeof thing !== 'object') {
    // the messiness below capitalizes the first letter, so the output matches
    // the other return values below. -JC
    var typeofThing = typeof thing; // edgecase of JSHint's "singleGroups"
    return typeofThing[0].toUpperCase() + typeofThing.slice(1);
  }
  else {
    if (thing && thing.constructor === Array) {
      return 'Array';
    }
    else if (thing === null) {
      return 'null';
    }
    else if (thing instanceof Date) {
      return 'Date';
    }
    else if (thing instanceof ObjectId) {
      return 'ObjectId';
    }
    else if (thing instanceof BinData) {
      var binDataTypes = {};
      binDataTypes[0x00] = 'generic';
      binDataTypes[0x01] = 'function';
      binDataTypes[0x02] = 'old';
      binDataTypes[0x03] = 'UUID';
      binDataTypes[0x05] = 'MD5';
      binDataTypes[0x80] = 'user';
      return 'BinData-' + binDataTypes[thing.subtype()];
    } else {
      return 'Object';
    }
  }
};

//flattens object keys to 1D. i.e. {'key1':1,{'key2':{'key3':2}}} becomes {'key1':1,'key2.key3':2}
//we assume no '.' characters in the keys, which is an OK assumption for MongoDB
var serializeDoc = function(doc, maxDepth) {
  var result = {};

  //determining if an object is a Hash vs Array vs something else is hard
  //returns true, if object in argument may have nested objects and makes sense to analyse its content
  function isHash(v) {
    var isArray = Array.isArray(v);
    var isObject = typeof v === 'object';
    var specialObject = v instanceof Date ||
                        v instanceof ObjectId ||
                        v instanceof BinData;
    return !specialObject && (isArray || isObject);
  }

  function serialize(document, parentKey, maxDepth){
    for(var key in document){
      //skip over inherited properties such as string, length, etch
      if(!document.hasOwnProperty(key)) {
        continue;
      }
      var value = document[key];
      //objects are skipped here and recursed into later
      //if(typeof value != 'object')
      result[parentKey+key] = value;
      //it's an object, recurse...only if we haven't reached max depth
      if(isHash(value) && maxDepth > 1) {
        serialize(value, parentKey+key+'.',maxDepth-1);
      }
    }
  }
  serialize(doc, '', maxDepth);
  return result;
};

// convert document to key-value map, where value is always an array with types as plain strings
var analyseDocument = function(document) {
  var result = {};
  for (var key in document) {
    var value = document[key];
    //translate unnamed object key from {_parent_name_}.{_index_} to {_parent_name_}.XX
    key = key.replace(/\.\d+/g,'.XX');
    if(typeof result[key] === 'undefined') {
      result[key] = {};
    }
    var type = varietyTypeOf(value);
    result[key][type] = true;
  }
  return result;
};

var mergeDocument = function(docResult, interimResults) {
  for (var key in docResult) {
    if(key in interimResults) {
      var existing = interimResults[key];
      for(var type in docResult[key]) {
        existing.types[type] = true;
      }
      existing.totalOccurrences = existing.totalOccurrences + 1;
    } else {
      interimResults[key] = {'types':docResult[key],'totalOccurrences':1};
    }
  }
};

var convertResults = function(interimResults, documentsCount) {
  var getKeys = function(obj) {
    var keys = [];
    for(var key in obj) {
      keys.push(key);
    }
    return keys.sort();
  };
  var varietyResults = [];
  //now convert the interimResults into the proper format
  for(var key in interimResults) {
    var entry = interimResults[key];
    varietyResults.push({
        '_id': {'key':key},
        'value': {'types':getKeys(entry.types)},
        'totalOccurrences': entry.totalOccurrences,
        'percentContaining': entry.totalOccurrences * 100 / documentsCount
    });
  }
  return varietyResults;
};

// Merge the keys and types of current object into accumulator object
var reduceDocuments = function(accumulator, object) {
  var docResult = analyseDocument(serializeDoc(object, maxDepth));
  mergeDocument(docResult, accumulator);
  return accumulator;
};

// We throw away keys which end in an array index, since they are not useful
// for our analysis. (We still keep the key of their parent array, though.) -JC
var filter = function(item) {
  return !item._id.key.match(/\.XX$/);
};

// sort desc by totalOccurrences or by key asc if occurrences equal
var comparator = function(a, b) {
  var countsDiff = b.totalOccurrences - a.totalOccurrences;
  return countsDiff !== 0 ? countsDiff : a._id.key.localeCompare(b._id.key);
};

// extend standard MongoDB cursor of reduce method - call forEach and combine the results


var interimResults = items.reduce(reduceDocuments, {});
var varietyResults = convertResults(interimResults, items.length)
  .filter(filter)
  .sort(comparator);

return varietyResults;

}
