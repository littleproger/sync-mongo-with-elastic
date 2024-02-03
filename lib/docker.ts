'use strict'
import { Sync } from './sync';
import {readFileSync} from 'fs';

const mongoDBUrl = process.env.MONGODB_URL_WITH_REPLICA_SET
const elasticUrl = process.env.ELASTIC_URL
const prefix = process.env.INDEX_PREFIX
const isDebug = process.env.IS_DEBUG
const withInitialSync = process.env.WITH_INITIAL_SYNC
const pathToCertificate = process.env.PATH_TO_CERTIFICATE
const isCertificateSelfSigned = process.env.IS_CERTIFICATE_SELF_SIGNED;

if(!mongoDBUrl) throw new Error('MONGODB_URL_WITH_REPLICA_SET is not provided!')
if(!elasticUrl) throw new Error('ELASTIC_URL is not provided!')
if(!prefix) throw new Error('INDEX_PREFIX is not provided!')
if(!isDebug) throw new Error('IS_DEBUG is not provided!')
if(!withInitialSync) throw new Error('WITH_INITIAL_SYNC is not provided!')
if(!pathToCertificate) throw new Error('PATH_TO_CERTIFICATE is not provided!')
if(!isCertificateSelfSigned) throw new Error('IS_CERTIFICATE_SELF_SIGNED is not provided!');

const autoSyncObject = new Sync(
    mongoDBUrl,
    elasticUrl,
    {
      prefix: prefix,
      debug: Boolean(isDebug),
      initialSync: Boolean(withInitialSync),
      tls:{
        ca: readFileSync(pathToCertificate),
        rejectUnauthorized: !Boolean(isCertificateSelfSigned)
      }
    }
);

(async ()=>{
  console.log("Started");
  const excludedCollections = ['pages', 'settings', 'root__schemas', 'migrations_changelog'];
  autoSyncObject.startSync(excludedCollections);
})()
