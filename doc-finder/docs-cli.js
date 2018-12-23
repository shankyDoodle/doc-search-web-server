#!/usr/bin/env node

'use strict';

const fs = require('fs');
const path = require('path');
const readline = require('readline');
const util = require('util');
const readFile = util.promisify(fs.readFile);

const DocFinder = require('./doc-finder');

//entry point
function main() {
  if (process.argv.length < 4) usage();
  (async (args) => await go(process.argv.slice(2)))();
}

module.exports = main;

/** Top level routine: args[0] is DB URL; args[1] is command; rest of
 *  args depend on command
 */
async function go(args) {
  let finder;
  try {
    if (!/^mongodb\:\/\/.+?\:\d+\/\w+$/.test(args[0])) {
      console.error('bad mongo-db url; must be of form ' +
		    'mongodb://SERVER:PORT/DBNAME');
      usage();
    }
    const cmd = args[1];
    const fn = COMMANDS[cmd];
    if (!fn) {
      console.error(`bad command '${args[1]}'`);
      usage();
    }
    finder = await DocFinder.create(args[0]);
    await fn(finder, args.slice(2));
  }
  catch (err) {
    console.error(err);
  }
  finally {
    if (finder) await finder.close();
  }
}

async function addContent(finder, args) {
  await add(finder, args, finder.addContent, true);
}

async function addNoiseWords(finder, args) {
  await add(finder, args, finder.addNoiseWords);
}

async function clear(finder, args) {
  if (args.length > 0) {
    console.error('clear does not require additional arguments.');
    usage();
  }
  await time(async () => await finder.clear());
}

async function complete(finder, args) {
  if (args.length === 0) {
    console.error('one-or-more completion terms are required');
    usage();
  }
  const text = args.join(' ');
  const completions = await time(async() => await finder.complete(text));
  if (completions.length === 0) {
    out('no completions\n');
  }
  else {
    completions.forEach(c => out(`${c}\n`));
  }
}

async function find(finder, args) {
  if (args.length === 0) {
    console.error('one-or-more search terms are required');
    usage();
  }
  const searchText = args.join(' ');
  const results = await time(async() => await finder.find(searchText));
  if (results.length === 0) {
    out('no results\n');
  }
  else {
    results.forEach((res) => out(`${res}\n`));
  }
}

async function docContent(finder, args) {
  if (args.length !== 1) {
    console.error('a single document name is required');
    usage();
  }
  try {
    const contents = await time(async() => await finder.docContent(args[0]));
    out(contents);
  }
  catch (err) {
    if (err.code === 'NOT_FOUND') {
      console.error(err.message);
    }
    else {
      throw err;
    }
  }
}

async function add(finder, args, fn, needsName=false) {
  if (args.length === 0) {
    console.error('one-or-more content names are required');
    usage();
  }
  await time(async() => {
    for (const fName of args) {
      const name = path.basename(fName, '.txt');
      const contents = await readFileContents(fName);
      const applyArgs = (needsName) ? [name, contents] : [contents];
      await fn.apply(finder, applyArgs);
    };
  });
}

async function readFileContents(path) {
  try {
    return await readFile(path, 'utf8');
  }
  catch (err) {
    console.error('cannot read %s: %s', path, err);
    process.exit(1);
  }
}

function out(text) { process.stdout.write(text); }

async function time(fn) {
  const t0 = Date.now();
  const ret = await fn();
  out(`time ${Date.now()-t0} milliseconds\n`);
  return ret;
}

function usage() {
  console.error(USAGE, path.basename(process.argv[1]));
  process.exit(1);
}

const USAGE = `
usage: %s DB_URL COMMAND [COMMAND_ARGS...]
  where COMMAND is:
  add-content CONTENT-FILE...
  add-noise NOISE-FILE...
  clear
  complete SEARCH-TERM...
  find SEARCH-TERM...
  get DOC_NAME
`.trim();

const COMMANDS = {
  ['add-content']: addContent,
  ['add-noise']: addNoiseWords,
  clear: clear,
  complete: complete,
  find: find,
  get: docContent,
}

main();

