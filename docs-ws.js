'use strict';

const cors = require('cors');
const express = require('express');
const bodyParser = require('body-parser');
const process = require('process');
const url = require('url');
const queryString = require('querystring');

const OK = 200;
const CREATED = 201;
const BAD_REQUEST = 400;
const NOT_FOUND = 404;
const CONFLICT = 409;
const SERVER_ERROR = 500;


//Main URLs
const DOCS = '/docs';
const COMPLETIONS = '/completions';

//Default value for count parameter
const COUNT = 5;

/**
 * Listen on port for incoming requests.  Use docFinder instance
 *  of DocFinder to access document collection methods.
 */
function serve(port, docFinder) {
  const app = express();
  app.locals.port = port;
  app.locals.finder = docFinder;
  setupRoutes(app);

  const server = app.listen(port, async function () {
    console.log(`PID ${process.pid} listening on port ${port}`);
  });

  return server;
}

module.exports = {serve};

function setupRoutes(app) {
  app.use(cors());            //for security workaround in future projects
  app.use(bodyParser.json()); //all incoming bodies are JSON

  app.get(DOCS, searchContent(app));
  app.post(DOCS, addContent(app));
  app.get(`${DOCS}/:id`, getContent(app));
  app.get(COMPLETIONS, getCompletions(app));

  app.use(doErrors()); //must be last; setup for server errors
}

/*************************** WEB Services Router Functions ****************************/

/**
 * @param app
 * @returns {*}
 * Get Document data by document name.
 */
function getContent(app) {
  return errorWrap(async function (req, res) {
    const id = req.params.id;

    try {
      const sContentData = await app.locals.finder.docContent(id);
      let oResult = {
        name: req.params.id,
        content: sContentData,
        links: _fGetLinksArray(req)
      }
      res.json(oResult);
    }
    catch (err) {
      err = Object.assign(err, {
        isDomain: true,
        errorCode: 'NOT_FOUND',
        message: `doc ${id} not found`,
      });
      const mapped = mapError(err);
      res.status(mapped.status).json(mapped);
    }
  });
}

/**
 * @param app
 * @returns {*}
 * Search a key in all documents in database and get result.
 */
function searchContent(app) {
  return errorWrap(async function (req, res) {
    const q = req.query || {};
    try {
      let oValidityData = _fCheckSearchQueryValidity(q);
      if (!oValidityData.isValid) {
        throw oValidityData;
      }

      const results = await app.locals.finder.find(q.q);
      res.json(_fGetSearchListResult(results, req));
    }
    catch (err) {
      const mapped = mapError(err);
      res.status(mapped.status).json(mapped);
    }
  });
}


/**
 * @param app
 * @returns {*}
 * Add new document using post method
 */
function addContent(app) {
  return errorWrap(async function (req, res) {
    try {
      const oReqBody = req.body;

      let oValidityData = _fCheckRequestBodyValidity(oReqBody);
      if (!oValidityData.isValid) {
        throw oValidityData;
      }

      const results = await app.locals.finder.addContent(oReqBody.name, oReqBody.content);

      let sPathName = req._parsedUrl.pathname + '/' + oReqBody.name;
      let sHost = req.headers.host;
      let sNewDocLink = _fGenerateLink(sHost, sPathName);

      res.append("Location", sNewDocLink);
      res.status(CREATED);
      res.json({"href": sNewDocLink});

    }
    catch (err) {
      const mapped = mapError(err);
      res.status(mapped.status).json(mapped);
    }
  });
}


/**
 * @param app
 * @returns {*}
 * Return a JSON list containing all the completions of the last word in TEXT
 */
function getCompletions(app) {
  return errorWrap(async function (req, res) {
    const q = req.query || {};
    try {
      const results = await app.locals.finder.complete(q.text);
      res.json(results);
    }
    catch (err) {
      const mapped = mapError(err);
      res.status(mapped.status).json(mapped);
    }
  });
}

/** Return error handler which ensures a server error results in nice
 *  JSON sent back to client with details logged on console.
 */
function doErrors(app) {
  return async function (err, req, res, next) {
    res.status(SERVER_ERROR);
    res.json({code: 'SERVER_ERROR', message: err.message});
    console.error(err);
  };
}

/** Set up error handling for handler by wrapping it in a
 *  try-catch with chaining to error handler on error.
 */
function errorWrap(handler) {
  return async (req, res, next) => {
    try {
      await handler(req, res, next);
    }
    catch (err) {
      next(err);
    }
  };
}


/** Return base URL of req for path.
 *  Useful for building links; Example call: baseUrl(req, DOCS)
 */
function baseUrl(req, path = '/') {
  const port = req.app.locals.port;
  const url = `${req.protocol}://${req.hostname}:${port}${path}`;
  return url;
}


/*************************** Mapping Errors ****************************/

const ERROR_MAP = {
  EXISTS: CONFLICT,
  NOT_FOUND: NOT_FOUND
}

/** Map domain/internal errors into suitable HTTP errors.  Return'd
 *  object will have a "status" property corresponding to HTTP status
 *  code.
 */
function mapError(err) {
  console.error(err);
  return err.isDomain
    ? {
      status: (ERROR_MAP[err.errorCode] || BAD_REQUEST),
      code: err.errorCode,
      message: err.message
    }
    : {
      status: SERVER_ERROR,
      code: 'INTERNAL',
      message: err.toString()
    };
}

/*************************** Private APIs ****************************/
function _fGenerateLink(sHost, sPathname, sSearchKey, iStart = 0, iCount = COUNT) {
  let sQueryData = "";
  if (!!sSearchKey) {
    sQueryData = `?q=${sSearchKey.replace(/ /g, "%20")}&start=${iStart}&count=${iCount}`;
  }
  return `http://${sHost}${sPathname}` + sQueryData;
}

function _fGetLinksArray(oRequestData, iTotalCount) {
  let sPathName = oRequestData._parsedUrl.pathname;
  let sHost = oRequestData.headers.host;

  let oQueryData = oRequestData.query;
  let sSearchKey = oQueryData.q;
  let iStart = +(oQueryData.start || 0);
  let iCount = +(oQueryData.count || COUNT);

  let aLinks = [];
  if (iTotalCount > 0) {
    if (sSearchKey && iStart > 0) {
      let iPrevStart = iStart - iCount;
      iPrevStart = iPrevStart < 0 ? 0 : iPrevStart;
      aLinks.push({
        rel: "Previous",
        href: _fGenerateLink(sHost, sPathName, sSearchKey, iPrevStart, iCount)
      });
    }

    aLinks.push({
      rel: "Self",
      href: _fGenerateLink(sHost, sPathName, sSearchKey, iStart, iCount)
    });

    if (sSearchKey && iStart + iCount < iTotalCount) {
      let iNextStart = iStart + iCount;
      aLinks.push({
        rel: "Next",
        href: _fGenerateLink(sHost, sPathName, sSearchKey, iNextStart, iCount)
      });
    }
  }

  return aLinks;
}

function _fGetSearchListResult(aResult, oRequestData) {
  let oFinalRes = {};

  let sPathName = oRequestData._parsedUrl.pathname;
  let sHost = oRequestData.headers.host;
  let oQueryData = oRequestData.query;
  let iStart = +(oQueryData.start || 0);
  let iCount = +(oQueryData.count || COUNT);
  let aSlicedRes = aResult.slice(iStart, iStart + iCount);
  aSlicedRes.forEach(function (oElement) {
    let sLocalPathName = sPathName + `/${oElement.name}`;
    oElement.href = _fGenerateLink(sHost, sLocalPathName);
  })
  oFinalRes.results = aSlicedRes;

  oFinalRes.totalCount = aResult.length;
  oFinalRes.links = _fGetLinksArray(oRequestData, aResult.length);

  return oFinalRes;
}

function _fCheckSearchQueryValidity(oQuery) {
  if (!oQuery.hasOwnProperty('q')) {
    return _fGetErrorDetailsData("BAD_PARAM", 'q');

  } else if (oQuery.hasOwnProperty('start') && (/*!+oQuery.start ||*/ +oQuery.start < 0)) {
    return _fGetErrorDetailsData("BAD_PARAM", 'start');

  } else if (oQuery.hasOwnProperty('count') && (/*!+oQuery.count ||*/ +oQuery.count < 0)) {
    return _fGetErrorDetailsData("BAD_PARAM", 'count');

  }

  return {isValid: true};
}

function _fCheckRequestBodyValidity(oRequestBody) {
  if (!oRequestBody.hasOwnProperty('name')) {
    return _fGetErrorDetailsData("BAD_REQUEST", 'name');

  } else if (!oRequestBody.hasOwnProperty('content')) {
    return _fGetErrorDetailsData("BAD_REQUEST", 'content');

  }
  return {isValid: true};
}

function _fGetErrorDetailsData(sErrorCode, sCulprit) {
  let oData = {};
  oData.isValid = false;
  oData.isDomain = true;
  oData.errorCode = sErrorCode;

  switch (sErrorCode) {
    case "BAD_PARAM":
      oData.message = sCulprit === "q" ? `required query parameter "${sCulprit}" is missing` : `bad query parameter "${sCulprit}"`;
      break;

    case "BAD_REQUEST":
      oData.message = `required body parameter "${sCulprit}" is missing`;
      break;
  }

  return oData;
}


