import { S3 as S3AWS } from 'aws-sdk';
import {
	DeleteObjectsOutput,
	DeleteObjectsRequest,
	GetObjectRequest,
	ListObjectsV2Request,
	Metadata,
	ObjectList,
	PutObjectRequest
} from 'aws-sdk/clients/s3';
import * as JSZip from 'jszip';
import { loadAsync } from 'jszip';
import { gunzipSync } from 'zlib';
import { logger } from './logger';

export class S3 {
	private readonly s3: S3AWS;

	constructor() {
		this.s3 = new S3AWS({ region: 'us-west-2' });
	}

	public async getObjectMetaData(bucketName: string, key: string): Promise<Metadata> {
		return new Promise<Metadata>(resolve => {
			const params: GetObjectRequest = {
				Bucket: bucketName,
				Key: key,
			};
			this.s3.getObject(params, (err, data) => {
				if (!!err || !data) {
					console.error('Could not load metadata', err, data);
					resolve(null);
				} else {
					resolve(data.Metadata);
				}
			});
		});
	}

	// Since S3 is only eventually consistent, it's possible that we try to read a file that is not
	// available yet
	public async readContentAsString(bucketName: string, key: string, retries = 10): Promise<string> {
		return new Promise<string>(resolve => {
			this.readContentAsStringInternal(bucketName, key, result => resolve(result), retries);
		});
	}

	private readContentAsStringInternal(bucketName: string, key: string, callback, retriesLeft: number) {
		if (retriesLeft <= 0) {
			console.error('could not read s3 object', bucketName, key);
			callback(null);
			return;
		}
		const input = { Bucket: bucketName, Key: key };
		this.s3.getObject(input, (err, data) => {
			if (err) {
				console.warn('could not read s3 object', bucketName, key, err, retriesLeft);
				setTimeout(() => {
					this.readContentAsStringInternal(bucketName, key, callback, retriesLeft - 1);
				}, 3000);
				return;
			}
			const objectContent = data.Body.toString('utf8');
			callback(objectContent);
		});
	}

	public async readGzipContent(bucketName: string, key: string, retries = 10): Promise<string> {
		return new Promise<string>(resolve => {
			this.readGzipContentInternal(bucketName, key, result => resolve(result), retries);
		});
	}

	private readGzipContentInternal(bucketName: string, key: string, callback, retriesLeft: number) {
		if (retriesLeft <= 0) {
			console.error('could not read s3 object', bucketName, key);
			callback(null);
			return;
		}
		const input = { Bucket: bucketName, Key: key };
		this.s3.getObject(input, (err, data) => {
			if (err) {
				console.warn('could not read s3 object', bucketName, key, err, retriesLeft);
				setTimeout(() => {
					this.readGzipContentInternal(bucketName, key, callback, retriesLeft - 1);
				}, 3000);
				return;
			}
			const result = gunzipSync(data.Body as any).toString('utf8');
			callback(result);
		});
	}

	public async readZippedContent(bucketName: string, key: string): Promise<string> {
		return new Promise<string>(resolve => {
			this.readZippedContentInternal(bucketName, key, result => resolve(result));
		});
	}

	private readZippedContentInternal(bucketName: string, key: string, callback, retriesLeft = 10) {
		logger.debug('trying to read zipped content', bucketName, key, retriesLeft);
		if (retriesLeft <= 0) {
			console.error('could not read s3 object', bucketName, key);
			callback(null);
			return;
		}
		const input = { Bucket: bucketName, Key: key };
		this.s3.getObject(input, async (err, data) => {
			if (err) {
				console.warn('could not read s3 object', bucketName, key, err, retriesLeft);
				setTimeout(() => {
					this.readZippedContentInternal(bucketName, key, callback, retriesLeft - 1);
				}, 1000);
				return;
			}
			try {
				logger.debug('success, loadAsync');
				const zipContent = await loadAsync(data.Body as any);
				logger.debug('zipContent loaded');
				const file = Object.keys(zipContent.files)[0];
				logger.debug('file retrieve', file);
				const objectContent = await zipContent.file(file).async('string');
				logger.debug('objectContent', objectContent);
				callback(objectContent);
			} catch (e) {
				console.warn('could not read s3 object', bucketName, key, err, retriesLeft, e);
				setTimeout(() => {
					this.readZippedContentInternal(bucketName, key, callback, retriesLeft - 1);
				}, 1000);
				return;
			}
		});
	}

	public async writeCompressedFile(content: any, bucket: string, fileName: string): Promise<boolean> {
		const jszip = new JSZip.default();
		jszip.file('replay.xml', content);
		const blob: Buffer = await jszip.generateAsync({
			type: 'nodebuffer',
			compression: 'DEFLATE',
			compressionOptions: {
				level: 9,
			},
		});
		return this.writeFile(blob, bucket, fileName, 'application/zip');
	}

	public async writeFile(
		content: any,
		bucket: string,
		fileName: string,
		type = 'application/json',
		encoding?: 'gzip' | null,
	): Promise<boolean> {
		return new Promise<boolean>((resolve, reject) => {
			const input: PutObjectRequest = {
				Body: type === 'application/json' && encoding !== 'gzip' ? JSON.stringify(content) : content,
				Bucket: bucket,
				Key: fileName,
				ACL: 'public-read',
				ContentType: type,
			};
			if (encoding) {
				input.ContentEncoding = encoding;
			}
			logger.debug('writing');
			this.s3.upload(input, (err, data) => {
				if (err) {
					console.error('could not upload file to S3', err, input);
					resolve(false);
					return;
				}
				resolve(true);
			});
		});
	}

	public async loadReplayString(replayKey: string): Promise<string> {
		return new Promise<string>(resolve => {
			this.loadReplayStringInternal(replayKey, replayString => resolve(replayString));
		});
	}

	private async loadReplayStringInternal(replayKey: string, callback, retriesLeft = 15): Promise<string> {
		if (retriesLeft <= 0) {
			console.error('Could not load replay xml', replayKey);
			callback(null);
			return;
		}
		const data = replayKey.endsWith('.zip')
			? await this.readZippedContent('xml.firestoneapp.com', replayKey)
			: await this.readContentAsString('xml.firestoneapp.com', replayKey);
		// const data = await http(`https://s3-us-west-2.amazonaws.com/xml.firestoneapp.com/${replayKey}`);
		// If there is nothing, we get the S3 "no key found" error
		if (!data || data.length < 5000) {
			setTimeout(() => this.loadReplayStringInternal(replayKey, callback, retriesLeft - 1), 500);
			return;
		}
		callback(data);
	}

	public async loadAllFileKeys(bucket: string, folder: string): Promise<ObjectList> {
		return new Promise<ObjectList>((resolve, reject) => {
			const request: ListObjectsV2Request = {
				Bucket: bucket,
				Prefix: folder,
			};
			this.s3.listObjectsV2(request, (err, data) => {
				resolve(data.Contents);
			});
		});
	}

	public async deleteFiles(bucket: string, keys: readonly string[]): Promise<DeleteObjectsOutput> {
		return new Promise<DeleteObjectsOutput>((resolve, reject) => {
			const request: DeleteObjectsRequest = {
				Bucket: bucket,
				Delete: {
					Objects: keys.map(key => ({
						Key: key,
					})),
				},
			};
			this.s3.deleteObjects(request, (err, data) => {
				resolve(data);
			});
		});
	}
}
