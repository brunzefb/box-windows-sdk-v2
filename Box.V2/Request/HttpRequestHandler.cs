using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading;
using System.Threading.Tasks;
using Box.V2.Config;
using Box.V2.Exceptions;

namespace Box.V2.Request
{
	public sealed class HttpRequestHandler : IRequestHandler
	{
		const HttpStatusCode TooManyRequests = (HttpStatusCode)429;

		public async Task<IBoxResponse<T>> ExecuteAsync<T>(IBoxRequest request)
			where T : class
		{
			// Need to account for special cases when the return type is a stream
			var isStream = typeof(T) == typeof(Stream);
			const int maxRetries = 30;
			var currentRetry = 0;

			try
			{
				// TODO: yhu@ better handling of different request
				var isMultiPartRequest = request.GetType() == typeof(BoxMultiPartRequest);
				var isBinaryRequest = request.GetType() == typeof(BoxBinaryRequest);

				while (true)
				{
					var httpRequest = BuildHttpRequestMessage(request, isMultiPartRequest, isBinaryRequest);
					AddHttpHeaders(request, httpRequest);

					// If we are retrieving a stream, we should return without reading the entire response
					var completionOption = isStream
						? HttpCompletionOption.ResponseHeadersRead
						: HttpCompletionOption.ResponseContentRead;

					Debug.WriteLine($"RequestUri: {httpRequest.RequestUri}");

					var client = GetClient(request);

					// Not disposing the reponse since it will affect stream response 
					var response =
						await client.SendAsync(httpRequest, completionOption).ConfigureAwait(false);

					//need to wait for Retry-After seconds and then retry request
					var retryAfterHeader = response.Headers.RetryAfter;

					// If we get a 429 error code and this is not a multi part request (often meaning a file upload, which cannot be retried
					// because the stream cannot be reset) and we haven't exceeded the number of allowed retries, then retry the request.
					// If we get a 202 code and has a retry-after header, we will retry after
					if (response.StatusCode == TooManyRequests && !isMultiPartRequest || 
					    response.StatusCode == HttpStatusCode.Accepted && retryAfterHeader != null)
					{
						var delay = GetExponentialBackoffDelayInMilliseconds(currentRetry);
						if (retryAfterHeader.Delta.HasValue)
						{
							// not sure if the server actually gives a 202 code with a retry.  Normally
							// server returns a 503 (Service Unavailable) or 301 (Moved Permanently) 
							delay = retryAfterHeader.Delta.Value;
						}

						Debug.WriteLine(
							$"HttpCode : {response.StatusCode}. Waiting for {delay.Seconds} seconds to retry request. RequestUri: {httpRequest.RequestUri}");

						await DelayWithProcessing((int)delay.TotalMilliseconds);

						if (currentRetry++ >= maxRetries)
						{
							var err = new BoxError
							{
								Code = response.StatusCode.ToString(),
								Description = "HTTP error 429: retries exceeded",
								Message = maxRetries + " retries were exceeded"
							};
							throw new BoxException(err.Message, err) { StatusCode = response.StatusCode, ResponseHeaders = response.Headers };
						}
					}
					else
					{
						var boxResponse = new BoxResponse<T>();
						boxResponse.Headers = response.Headers;

						// Translate the status codes that interest us 
						boxResponse.StatusCode = response.StatusCode;
						BoxResponseStatusFromHttpStatus(response, boxResponse);

						if (isStream && boxResponse.Status == ResponseStatus.Success)
						{
							var resObj = await response.Content.ReadAsStreamAsync().ConfigureAwait(false);
							boxResponse.ResponseObject = resObj as T;
						}
						else
						{
							boxResponse.ContentString =
								await response.Content.ReadAsStringAsync().ConfigureAwait(false);
						}

						return boxResponse;
					}
				}
			}
			catch (Exception ex)
			{
				Debug.WriteLine($"Exception: {ex.Message}");
				throw;
			}
		}

		private static TimeSpan GetExponentialBackoffDelayInMilliseconds(int currentRetryNumber)
		{
			var random = new Random();
			var retry = Math.Max(currentRetryNumber, 20);
			// limit so we don't get bigger than int
			// using ideas from https://www.awsarchitectureblog.com/2015/03/backoff.html
			var cap = (int)TimeSpan.FromMinutes(2).TotalSeconds;
			var delayInMilliseconds = random.Next(0, Math.Min(cap, (int)Math.Pow(2, retry) * 1000));
			return TimeSpan.FromMilliseconds(delayInMilliseconds);
			
		}

		private static void BoxResponseStatusFromHttpStatus<T>(HttpResponseMessage response,
			BoxResponse<T> boxResponse) where T : class
		{
			switch (response.StatusCode)
			{
				case HttpStatusCode.OK:
				case HttpStatusCode.Created:
				case HttpStatusCode.NoContent:
				case HttpStatusCode.Found:
				case HttpStatusCode.PartialContent: // Download with range
					boxResponse.Status = ResponseStatus.Success;
					break;
				case HttpStatusCode.Accepted:
					boxResponse.Status = ResponseStatus.Pending;
					break;
				case HttpStatusCode.Unauthorized:
					boxResponse.Status = ResponseStatus.Unauthorized;
					break;
				case HttpStatusCode.Forbidden:
					boxResponse.Status = ResponseStatus.Forbidden;
					break;
				case TooManyRequests:
					boxResponse.Status = ResponseStatus.TooManyRequests;
					break;
				default:
					boxResponse.Status = ResponseStatus.Error;
					break;
			}
		}

		private static void AddHttpHeaders(IBoxRequest request, HttpRequestMessage httpRequest)
		{
			foreach (var kvp in request.HttpHeaders)
			{
				// They could not be added to the headers directly
				if (kvp.Key == Constants.RequestParameters.ContentMD5
				    || kvp.Key == Constants.RequestParameters.ContentRange)
				{
					httpRequest.Content.Headers.Add(kvp.Key, kvp.Value);
				}
				else
				{
					httpRequest.Headers.TryAddWithoutValidation(kvp.Key, kvp.Value);
				}
			}
		}

		private HttpRequestMessage BuildHttpRequestMessage(IBoxRequest request,
			bool isMultiPartRequest, bool isBinaryRequest)
		{
			HttpRequestMessage httpRequest;
			if (isMultiPartRequest)
			{
				httpRequest = BuildMultiPartRequest(request as BoxMultiPartRequest);
			}
			else if (isBinaryRequest)
			{
				httpRequest = BuildBinaryRequest(request as BoxBinaryRequest);
			}
			else
			{
				httpRequest = BuildRequest(request);
			}
			return httpRequest;
		}

		private static class ClientFactory
		{
			private static readonly Lazy<HttpClient> autoRedirectClient =
				new Lazy<HttpClient>(() => CreateClient(true));

			private static readonly Lazy<HttpClient> nonAutoRedirectClient =
				new Lazy<HttpClient>(() => CreateClient(false));

			private static readonly IDictionary<TimeSpan, HttpClient> httpClientCache =
				new Dictionary<TimeSpan, HttpClient>();

			// reuseable HttpClient instance
			public static HttpClient AutoRedirectClient => autoRedirectClient.Value;
			public static HttpClient NonAutoRedirectClient => nonAutoRedirectClient.Value;

			// Create new HttpClient per timeout
			public static HttpClient CreateClientWithTimeout(bool followRedirect, TimeSpan timeout)
			{
				lock(httpClientCache)
				{
					if (!httpClientCache.ContainsKey(timeout))
					{
						// create new client with timeout
						var client = CreateClient(followRedirect);
						client.Timeout = timeout;
						// cache it
						httpClientCache.Add(timeout, client);
					}

					return httpClientCache[timeout];
				}
			}

			private static HttpClient CreateClient(bool followRedirect)
			{
				var handler = new HttpClientHandler
				{
					AutomaticDecompression = DecompressionMethods.Deflate | DecompressionMethods.GZip
				};
				handler.AllowAutoRedirect = followRedirect;

				return new HttpClient(handler);
			}
		}

		private HttpClient GetClient(IBoxRequest request)
		{
			HttpClient client;

			if (request.Timeout.HasValue)
			{
				var timeout = request.Timeout.Value;
				client = ClientFactory.CreateClientWithTimeout(request.FollowRedirect, timeout);
			}
			else
			{
				if (request.FollowRedirect)
				{
					client = ClientFactory.AutoRedirectClient;
				}
				else
				{
					client = ClientFactory.NonAutoRedirectClient;
				}
			}

			return client;
		}

		private HttpRequestMessage BuildRequest(IBoxRequest request)
		{
			var httpRequest = new HttpRequestMessage
			{
				RequestUri = request.AbsoluteUri,
				Method = GetHttpMethod(request.Method)
			};
			if (httpRequest.Method == HttpMethod.Get)
			{
				return httpRequest;
			}

			HttpContent content;

			// Set request content to string or form-data
			if (!string.IsNullOrWhiteSpace(request.Payload))
			{
				if (string.IsNullOrEmpty(request.ContentType))
				{
					content = new StringContent(request.Payload);
				}
				else
				{
					content = new StringContent(request.Payload, request.ContentEncoding,
						request.ContentType);
				}
			}
			else
			{
				content = new FormUrlEncodedContent(request.PayloadParameters);
			}

			httpRequest.Content = content;

			return httpRequest;
		}

		private HttpRequestMessage BuildBinaryRequest(BoxBinaryRequest request)
		{
			var httpRequest = new HttpRequestMessage();
			httpRequest.RequestUri = request.AbsoluteUri;
			httpRequest.Method = GetHttpMethod(request.Method);

			HttpContent content = null;

			var filePart = request.Part as BoxFilePart;
			if (filePart != null)
			{
				content = new StreamContent(filePart.Value);
			}

			httpRequest.Content = content;

			return httpRequest;
		}

		private HttpMethod GetHttpMethod(RequestMethod requestMethod)
		{
			switch (requestMethod)
			{
				case RequestMethod.Get:
					return HttpMethod.Get;
				case RequestMethod.Put:
					return HttpMethod.Put;
				case RequestMethod.Delete:
					return HttpMethod.Delete;
				case RequestMethod.Post:
					return HttpMethod.Post;
				case RequestMethod.Options:
					return HttpMethod.Options;
				default:
					throw new InvalidOperationException("Http method not supported");
			}
		}

		private HttpRequestMessage BuildMultiPartRequest(BoxMultiPartRequest request)
		{
			var httpRequest = new HttpRequestMessage(HttpMethod.Post, request.AbsoluteUri);
			var multiPart = new MultipartFormDataContent();

			// Break out the form parts from the request
			var filePart = request.Parts.Where(p => p.GetType() == typeof(BoxFileFormPart))
				.Select(p => p as BoxFileFormPart)
				.FirstOrDefault(); // Only single file upload is supported at this time
			var stringParts = request.Parts.Where(p => p.GetType() == typeof(BoxStringFormPart))
				.Select(p => p as BoxStringFormPart);

			// Create the string parts
			foreach (var sp in stringParts)
				multiPart.Add(new StringContent(sp?.Value), ForceQuotesOnParam(sp.Name));

			// Create the file part
			if (filePart != null)
			{
				var fileContent = new StreamContent(filePart.Value);
				fileContent.Headers.ContentDisposition = new ContentDispositionHeaderValue("form-data")
				{
					Name = ForceQuotesOnParam(filePart.Name),
					FileName = ForceQuotesOnParam(filePart.FileName)
				};
				multiPart.Add(fileContent);
			}

			httpRequest.Content = multiPart;

			return httpRequest;
		}

		/// <summary>
		/// Adds quotes around the named parameters
		/// This is required as the API will currently not take multi-part parameters without quotes
		/// </summary>
		/// <param name="name">The name parameter to add quotes to</param>
		/// <returns>The name parameter surrounded by quotes</returns>
		private string ForceQuotesOnParam(string name)
		{
			return $"\"{name}\"";
		}

		private static async Task DelayWithProcessing(int milliseconds)
		{
			var cancellationTokenSource = new CancellationTokenSource();
			var taskList = new List<Task>();
			for (var i = 0; i < 3; i++)
			{
				var task = new Task(() => { Fibonacci_Delay(cancellationTokenSource); });
				task.Start();
				taskList.Add(task);
			}
			taskList.Add(Task.Delay(TimeSpan.FromMilliseconds(milliseconds), cancellationTokenSource.Token));
			await Task.WhenAny(taskList.ToArray());
			cancellationTokenSource.Cancel();
		}

		private static void Fibonacci_Delay(CancellationTokenSource source)
		{
			var a = 0;
			var b = 1;

			while (true)
			{
				for (var i = 2; i < 10000; i++)
				{
					var c = a + b;
					a = b;
					b = c;
					
					if (source.IsCancellationRequested)
						return;
				}
			}
		}
	}
}