import { AWSError, S3 as S3AWS } from 'aws-sdk';
import {
	DeleteObjectsOutput,
	DeleteObjectsRequest,
	GetObjectRequest,
	ListObjectsV2Request,
	Metadata,
	ObjectCannedACL,
	ObjectList,
	PutObjectRequest,
} from 'aws-sdk/clients/s3';
import { PromiseResult } from 'aws-sdk/lib/request';
import * as JSZip from 'jszip';
import { loadAsync } from 'jszip';
import { Readable } from 'stream';
import { gunzipSync } from 'zlib';

export class S3 {
	private readonly s3: S3AWS;

	constructor(options?: { timeout?: number; connectTimeout?: number }) {
		this.s3 = new S3AWS({
			region: 'us-west-2',
			httpOptions: {
				timeout: options?.timeout ?? 2_000, // time succeed in starting the call
				connectTimeout: options?.connectTimeout ?? 3_000, // time to wait for a response
				// the aws-sdk defaults to automatically retrying
				// if one of these limits are met.
			},
		});
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

	public async readGzipContent(
		bucketName: string,
		key: string,
		retries = 10,
		logFileNotFound = true,
		timeout = 3000,
	): Promise<string> {
		return new Promise<string>(resolve => {
			this.readGzipContentInternal(bucketName, key, result => resolve(result), retries, logFileNotFound, timeout);
		});
	}

	private readGzipContentInternal(
		bucketName: string,
		key: string,
		callback,
		retriesLeft: number,
		logFileNotFound: boolean,
		timeout: number,
	) {
		if (retriesLeft <= 0) {
			if (logFileNotFound) {
				console.error('could not read s3 object', bucketName, key);
			}
			callback(null);
			return;
		}
		const input = { Bucket: bucketName, Key: key };
		this.s3.getObject(input, (err, data) => {
			if (err) {
				if (retriesLeft - 1 <= 0) {
					if (logFileNotFound) {
						console.error('could not read s3 object', bucketName, key);
					}
					callback(null);
					return;
				}
				if (logFileNotFound) {
					console.warn('could not read s3 object', bucketName, key, err, retriesLeft);
				}
				setTimeout(() => {
					this.readGzipContentInternal(bucketName, key, callback, retriesLeft - 1, logFileNotFound, timeout);
				}, timeout);
				return;
			}
			const result = gunzipSync(data.Body as any).toString('utf8');
			callback(result);
		});
	}

	public async readZippedContent(bucketName: string, key: string, retries = 3): Promise<string> {
		return new Promise<string>(resolve => {
			this.readZippedContentInternal(bucketName, key, result => resolve(result), retries);
		});
	}

	private readZippedContentInternal(bucketName: string, key: string, callback, retriesLeft = 10) {
		// console.debug('trying to read zipped content', bucketName, key, retriesLeft);
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
				// console.debug('success, loadAsync');
				const zipContent = await loadAsync(data.Body as any);
				// console.debug('zipContent loaded');
				const file = Object.keys(zipContent.files)[0];
				// console.debug('file retrieve', file);
				const objectContent = await zipContent.file(file).async('string');
				// console.debug('objectContent', objectContent?.substring(0, 200));
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
		// console.debug('created empty zip container');
		jszip.file('replay.xml', content);
		// console.debug('added content to zip container');
		const blob: Buffer = await jszip.generateAsync({
			type: 'nodebuffer',
			compression: 'DEFLATE',
			compressionOptions: {
				level: 9,
			},
		});
		// console.debug('file compressed');
		return this.writeFile(blob, bucket, fileName, 'application/zip');
	}

	public async writeArrayAsMultipart(
		content: any[],
		bucket: string,
		fileName: string,
		type = 'application/json',
		chunkSize = 50_000,
	): Promise<void> {
		const multipartCreateResult = await this.s3
			.createMultipartUpload({
				Bucket: bucket,
				Key: fileName,
				ContentType: type,
				ACL: 'public-read',
				StorageClass: 'STANDARD',
			})
			.promise();

		// let currentIndex = 0;
		let chunkCount = 1;
		const uploadPartResults = [];
		while (!!content.length) {
			const data = content.splice(0, chunkSize);
			// const data = content.slice(currentIndex, currentIndex + chunkSize);
			// console.log('uploading multipart data', chunkCount, ' ', data.length);
			const strBody = data.map(d => JSON.stringify(d)).join('\n');
			const uploadPromiseResult = await this.s3
				.uploadPart({
					Body: strBody,
					Bucket: bucket,
					Key: fileName,
					PartNumber: chunkCount,
					UploadId: multipartCreateResult.UploadId,
				})
				.promise();
			// console.log('multipart data upload result', uploadPromiseResult);

			uploadPartResults.push({
				PartNumber: chunkCount,
				ETag: uploadPromiseResult.ETag,
			});

			// currentIndex = currentIndex + chunkSize;
			chunkCount++;
		}

		const completeUploadResponse = await this.s3
			.completeMultipartUpload({
				Bucket: bucket,
				Key: fileName,
				MultipartUpload: {
					Parts: uploadPartResults,
				},
				UploadId: multipartCreateResult.UploadId,
			})
			.promise();
		// console.log('multipart upload complete', completeUploadResponse);
	}

	public readStream(bucketName: string, key: string): Readable {
		try {
			const stream = this.s3
				.getObject({
					Bucket: bucketName,
					Key: key,
				})
				.createReadStream();
			const finalStream = stream;
			return finalStream;
		} catch (e) {
			console.error('could not read stream', e, bucketName, key);
			return null;
		}
	}

	public async writeFile(
		content: any,
		bucket: string,
		fileName: string,
		type = 'application/json',
		encoding?: 'gzip' | null,
		ACL: ObjectCannedACL | null = 'public-read',
	): Promise<boolean> {
		return new Promise<boolean>((resolve, reject) => {
			try {
				const input: PutObjectRequest = {
					Body: type === 'application/json' && encoding !== 'gzip' ? JSON.stringify(content) : content,
					Bucket: bucket,
					Key: fileName,
					ACL: ACL,
					ContentType: type,
				};
				if (encoding) {
					input.ContentEncoding = encoding;
				}
				// console.debug('writing');
				this.s3.upload(input, (err, data) => {
					// console.debug('upload over', err, data);
					if (err) {
						console.error('could not upload file to S3', err, input);
						resolve(false);
						return;
					}
					resolve(true);
				});
			} catch (e) {
				console.error('Exception while writing file', e);
			}
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

export class S3Multipart {
	public get processing(): boolean {
		return this._processing;
	}

	private currentUpload: PromiseResult<S3AWS.CreateMultipartUploadOutput, AWSError>;
	private currentPart = 1;
	private uploadPartResults: { PartNumber: number; ETag: string }[] = [];
	private _processing = false;

	constructor(private readonly s3: S3AWS) {}

	public initMultipart = async (bucket: string, fileName: string, type = 'application/json') => {
		this._processing = true;
		this.currentUpload = await this.s3
			.createMultipartUpload({
				Bucket: bucket,
				Key: fileName,
				ContentType: type,
				ACL: 'public-read',
				StorageClass: 'STANDARD',
			})
			.promise();
		this._processing = false;
	};

	public uploadPart = async (content: string) => {
		this._processing = true;
		const result = await this.s3
			.uploadPart({
				Body: content,
				Bucket: this.currentUpload.Bucket,
				Key: this.currentUpload.Key,
				PartNumber: this.currentPart,
				UploadId: this.currentUpload.UploadId,
			})
			.promise();
		this.uploadPartResults.push({
			PartNumber: this.currentPart,
			ETag: result.ETag,
		});
		this.currentPart++;
		this._processing = false;
	};

	public completeMultipart = async () => {
		this._processing = true;
		// console.log(
		// 	'completing multipart upload',
		// 	this.uploadPartResults,
		// 	this.currentUpload.Bucket,
		// 	this.currentUpload.Key,
		// 	this.currentUpload.UploadId,
		// );
		await this.s3
			.completeMultipartUpload({
				Bucket: this.currentUpload.Bucket,
				Key: this.currentUpload.Key,
				MultipartUpload: {
					Parts: this.uploadPartResults,
				},
				UploadId: this.currentUpload.UploadId,
			})
			.promise();
		this._processing = false;
	};
}
