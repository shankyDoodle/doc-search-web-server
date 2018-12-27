#!/usr/bin/env node

'use strict';

const {promisify} = require('util');
const readFile = promisify(require('fs').readFile);
const writeFile = promisify(require('fs').writeFile);
const Path = require('path');

const DocFinder = require('doc-finder');
const docsServer = require('./docs-ws');

function usage() {
  console.error("usage: %s MONGO_DB_URL PORT NOISE_FILE [CONTENT_FILE...]",
		Path.basename(process.argv[1]));
  process.exit(1);
}

function getPort(portArg) {
  let port = Number(portArg);
  if (!port) {
    console.error(`bad port '${portArg}'`);
    usage();
  }
  return port;
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

async function addContent(finder, args) {
  for (const fName of args) {
    const name = Path.basename(fName, '.txt');
    const text = await readFileContents(fName);
    await finder.addContent(name, text);
  }
}

async function shutdown(event, resources) {
  if (Object.keys(resources).length > 0) {
    console.log(`shutting down on ${event}`);
    if (resources.server) {
      await resources.server.close();
      delete resources.server;
    }
    if (resources.finder) {
      await resources.finder.close();
      delete resources.finder;
    }
    if (resources.timer) {
      clearInterval(resources.timer);
      delete resources.timer;
    }
  }
}

function cleanupResources(resources) {
  const events = [ 'SIGINT', 'SIGTERM', 'exit' ];
  for (const event of events) {
    process.on(event, async () => await shutdown(event, resources));
  }
}

const PID_FILE = '.pid';

//Time after which db should be reset
const CLEAR_TIME_MILLIS = Number(process.env.DOCS_CLEAR_TIME)*1000 || -1;

//Reset data in db, assuming args[2] contains name of noise-words file
//and remaining args contain names of content files.
async function resetData(finder, args) {
  await finder.clear();
  const noise = await readFileContents(args[2]);
  await finder.addNoiseWords(noise);
  await addContent(finder, args.slice(3));
}

//args[0]: mongdb url; args[1]: port; args[2]: name of noise words file.
//optional args[3]... : name of content files.
async function go(args) {
  const resources = {};
  try {

    /**Support for custom as well as mongoAtlas Cluster */
    if (args[0] != "mongoAtlas" && !/^mongodb\:\/\/.+?\:\d+\/\w+$/.test(args[0])) {
      console.error('bad mongo-db url; must be of form ' +
		    'mongodb://SERVER:PORT/DBNAME');
      usage();
    }
    const port = getPort(args[1]);
    const finder = resources.finder = await DocFinder.create(args[0]);
    await writeFile(PID_FILE, `${process.pid}\n`);
    await resetData(finder, args);
    resources.server = docsServer.serve(port, finder);
    if (CLEAR_TIME_MILLIS > 0) {
      const resetFn = async () => { await resetData(finder, args); }
      resources.timer = setInterval(resetFn, CLEAR_TIME_MILLIS)
    }
  }
  catch (err) {
    console.error(err);
  }
  finally {
    cleanupResources(resources);
  }
}

if (process.argv.length < 5) usage();
go(process.argv.slice(2));

// go(["mongodb://localhost:27017/docs", "1235", "./data/noise-words.txt", "./data/snark/*.txt"]);//TODO: remove this before committing