const assert = require('assert');
const mongo = require('mongodb').MongoClient;

const {inspect} = require('util'); //for debugging

'use strict';

/** This class is expected to persist its state.  Hence when the
 *  class is created with a specific database url, it is expected
 *  to retain the state it had when it was last used with that URL.
 */ 
class DocFinder {

  /** Constructor for instance of DocFinder. The dbUrl is
   *  expected to be of the form mongodb://SERVER:PORT/DB
   *  where SERVER/PORT specifies the server and port on
   *  which the mongo database server is running and DB is
   *  name of the database within that database server which
   *  hosts the persistent content provided by this class.
   */
  constructor(mongoUrl, dbName, client) {
    this.mongoUrl = mongoUrl; this.dbName = dbName;
    this.client = client;
    this.db = this.client.db(this.dbName);
    this.completionsTable = this.db.collection(COMPLETIONS_TABLE);
    this.contentsTable = this.db.collection(CONTENTS_TABLE);
    this.noiseTable = this.db.collection(NOISE_TABLE);
    this.wordsTable = this.db.collection(WORDS_TABLE);
    this.noiseWords = new Set();
  }

  /** This factory method creates and returns a new instance of
   *  DocFinder.
   */
  static async create(dbUrl) {
    const [, mongoUrl, dbName] = dbUrl.match(/^(.+?)\/(\w+)$/);
    const client = await mongo.connect(mongoUrl, MONGO_OPTIONS);
    const finder = new DocFinder(mongoUrl, dbName, client);
    await finder.db.createCollection(NOISE_TABLE);
    finder.noiseWords = await finder._readNoiseWords();
    return finder;
  }

  /** Release all resources held by this doc-finder.  Specifically,
   *  close any database connections.
   */
  async close() {
    await this.client.close();
  }

  /** Clear database */
  async clear() {
    await this.completionsTable.deleteMany({});
    await this.contentsTable.deleteMany({});
    await this.noiseTable.deleteMany({});
    await this.wordsTable.deleteMany({});
  }

  /** Return an array of non-noise normalized words from string
   *  contentText.  Non-noise means it is not a word in the noiseWords
   *  which have been added to this object.  Normalized means that
   *  words are lower-cased, have been stemmed and all non-alphabetic
   *  characters matching regex [^a-z] have been removed.
   */
  async words(contentText) {
    return (await this._wordsLow(contentText)).map((pair) => pair[0]);
  }

  /** Add all normalized words in the noiseText string to this as
   *  noise words.  This operation should be idempotent.
   */
  async addNoiseWords(noiseText) {
    const noiseWords = await this.words(noiseText);
    const noise = new Set(noiseWords);
    this.noiseWords.forEach(n => noise.delete(n));
    if (noise.size > 0) {
      const a = Array.from(noise);
      const noiseDocs = a.map(n=>({_id: n}));
      await this.noiseTable.insertMany(noiseDocs);
      a.forEach(n=>this.noiseWords.add(n));
    }
  }

  /** Add document named by string name with specified content string
   *  contentText to this instance. Update index in this with all
   *  non-noise normalized words in contentText string.
   *  This operation should be idempotent.
   */ 
  async addContent(name, contentText) {
    if (!contentText.endsWith('\n')) contentText += '\n';
    await this._saveContents(name, contentText);
    const index = await this._makeIndex(contentText);
    await this._saveWords(name, index);
    await this._updateCompletions(Object.keys(index));
  }

  /** Return contents of document name.  If not found, throw an Error
   *  object with property code set to 'NOT_FOUND' and property
   *  message set to `doc ${name} not found`.
   */
  async docContent(name) {
    const doc = await this.contentsTable.findOne({_id: name});
    if (doc) {
      return doc.contents;
    }
    else {
      const err = new Error(`doc ${name} not found`);
      err.code = 'NOT_FOUND';
      throw err;
    }
  }
  
  /** Given a text String containing search-terms (which may contain
   *  noise words), return a list of Result's which specify the
   *  matching documents.  Each Result object contains the following
   *  properties:
   *
   *     name:  the name of the document.
   *     score: the total number of occurrences of the search terms in the
   *            document.
   *     lines: A string consisting the lines containing the earliest
   *            occurrence of the search terms within the document.  The 
   *            lines must have the same relative order as in the source
   *            document.  Note that if a line contains multiple search 
   *            terms, then it will occur only once in lines.
   *
   *  The returned Result list must be sorted in non-ascending order
   *  by score.  Results which have the same score are sorted by the
   *  document name in lexicographical ascending order.
   *
   */
  async find(text) {
    const terms = Array.from(new Set(await this.words(text)));
    const docs = await this._findDocs(terms);
    const results = [];
    for (const [name, wordInfos] of docs.entries()) {
      const doc = await this.docContent(name);
      const score =
	wordInfos.reduce((acc, wordInfo) => acc + wordInfo[0], 0);
      const offsets = wordInfos.map(wordInfo => wordInfo[1]);
      results.push(new OffsetResult(name, score, offsets).result(doc));
    }
    results.sort(compareResults);
    return results;
  }

  /** Given a text string, return a ordered list of all completions of
   *  the last normalized word in text.  Returns [] if the last char
   *  in text is not alphabetic.
   */
  async complete(text) {
    if (!text.match(/[a-zA-Z]$/)) return [];
    const word = text.split(/\s+/).map(w=>normalize(w)).slice(-1)[0];
    const doc = await this.completionsTable.findOne({_id: word[0]});
    const completions = (doc) ? doc.words : [];
    completions.sort();
    return completions.filter((w) => w.startsWith(word));
  }

  /** Given a wordIndex for document name, save index info
   *  in db.
   */
  async _saveWords(name, wordsIndex) {
    const options = { upsert: true };
    for (const [word, wordInfo] of Object.entries(wordsIndex)) {
      const filter = { _id: word };
      const update = { $set: { [name]: wordInfo } };
      await this.wordsTable.updateOne(filter, update, options);
    }
  }

  /** Given a contentText string, return a index for each
   *  non-noise normalized word in contentText.  The return'd
   *  index is a object mapping each word to a pair
   *  [count, offset] where count is a count of the number
   *  of occurrences of word in contentText and offset is
   *  the offset of its first occurrence in contentText.
   */
  async _makeIndex(contentText) {
    const index = {};
    const words = await this._wordsLow(contentText);
    words.forEach((pair) => {
      const [word, offset] = pair;
      const wordInfo = index[word] || [0, offset];
      wordInfo[0]++;
      index[word] = wordInfo;
    });
    return index;
  }

  /** Save entire content contentsText for document name in db. */
  async _saveContents(name, contentsText) {
    const filter = { _id: name };
    const doc = { _id: name, contents: contentsText };
    await this.contentsTable.replaceOne(filter, doc, { upsert: true });
  }
  
  /** Given a list of words update the completions stored in the
   *  db with words.
   */
  async _updateCompletions(words) {
    const completions = this._makeCompletions(words);
    const options = { upsert: true };
    for (const c of completions.keys()) {
      const doc = await this.completionsTable.findOne({_id: c});
      const previous = (doc) ? doc.words : [];
      const cWords = Array.from(new Set(completions.get(c).concat(previous)));
      const update = {_id: c, words: cWords };
      await this.completionsTable.replaceOne({_id: c}, update, options);
    }
  }

  /** Return a map from characters to a list of all words in words
   *  which start with that character.
   */
  _makeCompletions(words) {
    const completions = new Map();
    for (const word of words) {
      const c = word[0];
      if (!completions.get(c)) completions.set(c, []);
      completions.get(c).push(word);
    }
    return completions;
  }

  /** Like words(), except that it returns a list of pairs with
   *  pair[0] containing the word and pair[1] containing the
   *  offset within content where the word starts.
   */
  async _wordsLow(content) {
    const words = [];
    let match;
    while (match = WORD_REGEX.exec(content)) {
      const word = normalize(match[0]);
      if (word && !this.noiseWords.has(word)) {
	words.push([word, match.index]);
      }
    }
    return words;
  }

  /** Return a set of all noise words read from the db. */
  async _readNoiseWords() {
    const cursor = await this.noiseTable.find({});
    const results = await cursor.toArray();
    return new Set(results.map(r => r._id));
  }

  /** Give a list of non-noise normalized terms, return a map from
   *  document name to a list of pairs where pair[0] contains a count
   *  of the number of occurences of a term from terms and pair[1]
   *  contains the offset at which the term occurs in the document
   *  content.
   */
  async _findDocs(terms) {
    const docs = new Map();
    for (const term of terms) {
      const termIndex = await this.wordsTable.findOne({_id: term});
      if (termIndex) {
	for (const [name, idx] of Object.entries(termIndex)) {
	  if (name === '_id') continue;
	  let docIndex = docs.get(name);
	  if (!docIndex) docs.set(name, docIndex = []);
	  docIndex.push(idx);
	}
      }
    };
    return docs;
  }

} //class DocFinder

module.exports = DocFinder;

//Collection names
const CONTENTS_TABLE = 'contents';
const COMPLETIONS_TABLE = 'completions';
const WORDS_TABLE = 'words';
const NOISE_TABLE = 'noise';

//Used to prevent warning messages from mongodb.
const MONGO_OPTIONS = {
  useNewUrlParser: true
};

/** Regex used for extracting words as maximal non-space sequences. */
const WORD_REGEX = /\S+/g;

/** A simple utility class which packages together the result for a
 *  document search as documented above in DocFinder.find().
 */ 
class Result {
  constructor(name, score, lines) {
    this.name = name; this.score = score; this.lines = lines;
  }

  toString() { return `${this.name}: ${this.score}\n${this.lines}`; }
}

/** Compare result1 with result2: higher scores compare lower; if
 *  scores are equal, then lexicographically earlier names compare
 *  lower.
 */
function compareResults(result1, result2) {
  return (result2.score - result1.score) ||
    result1.name.localeCompare(result2.name);
}

/** Normalize word by stem'ing it, removing all non-alphabetic
 *  characters and converting to lowercase.
 */
function normalize(word) {
  return stem(word.toLowerCase()).replace(/[^a-z]/g, '');
}

/** Place-holder for stemming a word before normalization; this
 *  implementation merely removes 's suffixes.
 */
function stem(word) {
  return word.replace(/\'s$/, '');
}

/** Like Result, except that instead of lines it contains a list of
 *  offsets at which the search terms occur within the document.
 */
class OffsetResult {
  constructor(name, score, offsets) {
    this.name = name; this.score = score; this.offsets = offsets;
  }

  /** Convert this to a Result by using this.offsets to extract
   *  lines from contents.
   */ 
  result(contents) {
    const starts = new Set();
    this.offsets.forEach(o => starts.add(contents.lastIndexOf('\n', o) + 1));
    let lines = [];
    for (const i of Array.from(starts).sort((a, b) => a-b)) {
      lines.push(contents.substring(i, contents.indexOf('\n', i) + 1));
    }
    return new Result(this.name, this.score, lines);
  }
}

