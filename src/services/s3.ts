import {
	CopyObjectCommand,
	CopyObjectCommandInput,
	CreateMultipartUploadCommandOutput,
	DeleteObjectsCommand,
	DeleteObjectsCommandInput,
	DeleteObjectsCommandOutput,
	GetObjectCommand,
	GetObjectCommandInput,
	GetObjectCommandOutput,
	GetObjectRequest,
	ListObjectsV2Command,
	ListObjectsV2CommandInput,
	ListObjectsV2CommandOutput,
	ListObjectsV2Output,
	ObjectCannedACL,
	PutObjectCommand,
	PutObjectCommandInput,
	S3 as S3AWSv3,
} from '@aws-sdk/client-s3';
import { NodeHttpHandler } from '@smithy/node-http-handler';
import * as JSZip from 'jszip';
import { loadAsync } from 'jszip';
import { Readable } from 'stream';
import { gunzipSync } from 'zlib';

export class S3 {
	private readonly s3: S3AWSv3;

	constructor(options?: { timeout?: number; connectTimeout?: number }) {
		this.s3 = new S3AWSv3({
			region: 'us-west-2',
			requestHandler: new NodeHttpHandler({
				connectionTimeout: options?.connectTimeout ?? 3_000,
				requestTimeout: options?.timeout ?? 2_000,
			}),
		});
	}

	public async getObjectMetaData(bucketName: string, key: string): Promise<GetObjectCommandOutput['Metadata']> {
		return new Promise<GetObjectCommandOutput['Metadata']>(resolve => {
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

	private async readGzipContentInternal(
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
		const input: GetObjectCommandInput = { Bucket: bucketName, Key: key };
		try {
			const data = await this.s3.send(new GetObjectCommand(input));
			const buffer = await streamToBuffer(data.Body as Readable);
			const result = gunzipSync(buffer).toString('utf8');
			callback(result);
		} catch (err) {
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
		}
	}

	public async readZippedContent(bucketName: string, key: string, retries = 3): Promise<string> {
		return new Promise<string>(resolve => {
			this.readZippedContentInternal(bucketName, key, result => resolve(result), retries);
		});
	}

	private async readZippedContentInternal(bucketName: string, key: string, callback, retriesLeft = 10) {
		if (retriesLeft <= 0) {
			console.error('could not read s3 object', bucketName, key);
			callback(null);
			return;
		}
		const input: GetObjectCommandInput = { Bucket: bucketName, Key: key };
		try {
			const data = await this.s3.send(new GetObjectCommand(input));
			const buffer = await streamToBuffer(data.Body as Readable);
			const zipContent = await loadAsync(buffer);
			const file = Object.keys(zipContent.files)[0];
			const objectContent = await zipContent.file(file).async('string');
			callback(objectContent);
		} catch (err) {
			console.warn('could not read s3 object', bucketName, key, err, retriesLeft);
			setTimeout(() => {
				this.readZippedContentInternal(bucketName, key, callback, retriesLeft - 1);
			}, 1000);
		}
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
		const multipartCreateResult = await this.s3.createMultipartUpload({
			Bucket: bucket,
			Key: fileName,
			ContentType: type,
			ACL: 'public-read',
			StorageClass: 'STANDARD',
		});

		// let currentIndex = 0;
		let chunkCount = 1;
		const uploadPartResults = [];
		while (!!content.length) {
			const data = content.splice(0, chunkSize);
			// const data = content.slice(currentIndex, currentIndex + chunkSize);
			// console.log('uploading multipart data', chunkCount, ' ', data.length);
			const strBody = data.map(d => JSON.stringify(d)).join('\n');
			const uploadPromiseResult = await this.s3.uploadPart({
				Body: strBody,
				Bucket: bucket,
				Key: fileName,
				PartNumber: chunkCount,
				UploadId: multipartCreateResult.UploadId,
			});
			// console.log('multipart data upload result', uploadPromiseResult);

			uploadPartResults.push({
				PartNumber: chunkCount,
				ETag: uploadPromiseResult.ETag,
			});

			// currentIndex = currentIndex + chunkSize;
			chunkCount++;
		}

		const completeUploadResponse = await this.s3.completeMultipartUpload({
			Bucket: bucket,
			Key: fileName,
			MultipartUpload: {
				Parts: uploadPartResults,
			},
			UploadId: multipartCreateResult.UploadId,
		});
		// console.log('multipart upload complete', completeUploadResponse);
	}

	public async readStream(bucketName: string, key: string): Promise<Readable | null> {
		try {
			const command = new GetObjectCommand({
				Bucket: bucketName,
				Key: key,
			});
			const response = await this.s3.send(command);
			const stream = response.Body as Readable;
			return stream;
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
		try {
			const input: PutObjectCommandInput = {
				Body: type === 'application/json' && encoding !== 'gzip' ? JSON.stringify(content) : content,
				Bucket: bucket,
				Key: fileName,
				ACL: ACL,
				ContentType: type,
			};
			if (encoding) {
				input.ContentEncoding = encoding;
			}

			const command = new PutObjectCommand(input);
			const response = await this.s3.send(command);
			return response.$metadata.httpStatusCode === 200;
		} catch (e) {
			console.error('Exception while writing file', e);
		}
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

	public async loadAllFileKeys(bucket: string, folder: string): Promise<ListObjectsV2Output['Contents']> {
		const request: ListObjectsV2CommandInput = {
			Bucket: bucket,
			Prefix: folder,
		};
		try {
			const command = new ListObjectsV2Command(request);
			const data: ListObjectsV2CommandOutput = await this.s3.send(command);
			return data.Contents || [];
		} catch (err) {
			console.error('could not list objects in S3', err);
			return [];
		}
	}

	public async deleteFiles(bucket: string, keys: readonly string[]): Promise<DeleteObjectsCommandOutput> {
		const request: DeleteObjectsCommandInput = {
			Bucket: bucket,
			Delete: {
				Objects: keys.map(key => ({
					Key: key,
				})),
			},
		};

		const command = new DeleteObjectsCommand(request);
		const data = await this.s3.send(command);
		return data;
	}

	public async copy(originBucket: string, originKey: string, destinationBucket: string, destinationKey: string) {
		const params: CopyObjectCommandInput = {
			Bucket: destinationBucket,
			CopySource: `${originBucket}/${originKey}`,
			Key: destinationKey,
		};
		try {
			const command = new CopyObjectCommand(params);
			const data = await this.s3.send(command);
			return data.$metadata.httpStatusCode === 200;
		} catch (err) {
			console.error('could not copy object', err, params);
			return false;
		}
	}
}

export class S3Multipart {
	public get processing(): boolean {
		return this._processing;
	}

	private currentUpload: CreateMultipartUploadCommandOutput;
	private currentPart = 1;
	private uploadPartResults: { PartNumber: number; ETag: string }[] = [];
	private _processing = false;

	constructor(private readonly s3: S3AWSv3) {}

	public initMultipart = async (bucket: string, fileName: string, type = 'application/json') => {
		this._processing = true;
		this.currentUpload = await this.s3.createMultipartUpload({
			Bucket: bucket,
			Key: fileName,
			ContentType: type,
			ACL: 'public-read',
			StorageClass: 'STANDARD',
		});
		this._processing = false;
	};

	public uploadPart = async (content: string) => {
		this._processing = true;
		const result = await this.s3.uploadPart({
			Body: content,
			Bucket: this.currentUpload.Bucket,
			Key: this.currentUpload.Key,
			PartNumber: this.currentPart,
			UploadId: this.currentUpload.UploadId,
		});
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
		await this.s3.completeMultipartUpload({
			Bucket: this.currentUpload.Bucket,
			Key: this.currentUpload.Key,
			MultipartUpload: {
				Parts: this.uploadPartResults,
			},
			UploadId: this.currentUpload.UploadId,
		});
		this._processing = false;
	};
}

// Utility function to convert stream to buffer
const streamToBuffer = async (stream: Readable): Promise<Buffer> => {
	return new Promise((resolve, reject) => {
		const chunks: Buffer[] = [];
		stream.on('data', chunk => chunks.push(chunk));
		stream.on('end', () => resolve(Buffer.concat(chunks)));
		stream.on('error', reject);
	});
};
