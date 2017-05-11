package com.delta.drc.hive.dao;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * 負責處理OpenStack的CRUD
 * 
 * 含swift官方API文件的URL {account} -> AUTH_{project hashed id}，這裡保留跟官方文件一樣的寫法，但實際上是左邊講的那樣
 * 
 * @author RYAN.PT.CHEN
 * @uml.annotations 
 *    uml_generalization="mmi:///#jsrctype^name=AbstractStorage[jcu^name=AbstractStorage.java[jpack^name=com.delta.drc.hive.dao[jsrcroot^srcfolder=src/main/java[project^id=DataHive]]]]$uml.Class"
 *
 */
public class OpenStackStorage extends AbstractStorage {
	/**
	 * TODO: 先用這個等pom.xml確定再改
	 */
    private static final Logger logger = Logger.getLogger(OpenStackStorage.class.getName());
    
    private static final SimpleDateFormat containerNameSDF = new SimpleDateFormat("yyyyMM");
    
	/**
	 * 看起來目前會用到的功能只有login需要post JSON文件
	 * 鑒於上次pom.xml加dependency RTC就一直報錯，小弟無能不敢動高貴的RTC會抱怨的東西
	 * TODO: 再確定pom.xml有哪個JSON的東西可用之後再改
	 */
	private static final String LOGIN_TEMPLATE = "{ \"auth\": { \"identity\": { \"methods\": [\"password\"], \"password\": { \"user\": { \"name\": \"%s\", \"domain\": { \"id\": \"default\" }, \"password\": \"%s\" } } }, \"scope\": { \"project\": { \"name\": \"%s\", \"domain\": { \"id\": \"default\" } } } } }"; 
	
	/**
	 * X-Subject-Token from KeyStone login reponse Header
	 * This token used in all KeyStone enabled openstack service api, request header "X-Auth-Token"
	 */
	private String authToken;
	
	private static final String KEYSTONE_AUTHSUBJECT_HEADER_NAME = "X-Subject-Token";
	private static final String SWIFT_AUTHHEADER_NAME = "X-Auth-Token";
	
	private Map<String, Object> config = null;

	protected OpenStackStorage(Connector connector) {
		super(connector);
		
	}
	
	private String getConfigValue(String key) {
		if(null == key || "".equals(key)) {
			logger.warning("key is empty");
			return null;
		}
		if(config == null) {
			if(connector != null) {
				config = this.connector.getConfig().getParameters();
			} else {
				logger.severe("connector is null, i cannot load any config to execute.");
			}
		}
		return config.get(key).toString();
	}
	
	private String getContainerName() {
		return getContainerName(new Date(System.currentTimeMillis()));
	}
	
	private String getContainerName(Date date) {
		if(null == date) {
			date = new Date(System.currentTimeMillis());
		}
		return containerNameSDF.format(date);
	}

	/**
	 * TODO: openStackLoginProtocol is reserved for change
	 * related config key: openStackLoginHost	openStackLoginPort	openStackLoginProtocol	openStackLoginUrl account password swiftProject
	 */
	public boolean login() {
		logger.info("login invoked");
		try {
			// TODO: config check
			String openStackLoginHost = getConfigValue("openStackLoginHost");
			String openStackLoginPort = getConfigValue("openStackLoginPort");
			String openStackLoginUrl = getConfigValue("openStackLoginUrl");
			String account = getConfigValue("account");
			String password = getConfigValue("password");
			String swiftProject = getConfigValue("swiftProject");
			logger.info(String.format("%s try login openstack", account));
			URL url = new URL(String.format("http://%s:%s/%s", openStackLoginHost, openStackLoginPort, openStackLoginUrl));
			HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			conn.setDoOutput(true);
			conn.setRequestMethod("POST");
			conn.setRequestProperty("Content-Type", "application/json");
			OutputStream os = conn.getOutputStream();
			String payload = String.format(LOGIN_TEMPLATE, account, password, swiftProject);
			logger.info(payload);
			os.write(payload.getBytes("UTF-8"));
			os.flush();
			if(conn.getResponseCode() != HttpURLConnection.HTTP_CREATED) {
				// login failed
				logger.warning("Login Failed");
				authToken = null;
				return false;
			} else {
				authToken = conn.getHeaderField(KEYSTONE_AUTHSUBJECT_HEADER_NAME);
				logger.info(authToken);
			}
			
			conn.disconnect();
		} catch (MalformedURLException e) {
			logger.log(Level.SEVERE, e.getMessage(), e);
			e.printStackTrace();
		} catch (IOException e) {
			logger.log(Level.SEVERE, e.getMessage(), e);
			e.printStackTrace();
		}
		return true;
	}
	
	private String makeSwiftContainerUrl() {
		String openStackSwiftUrl = getConfigValue("openStackSwiftUrl");
		if(openStackSwiftUrl.endsWith("/")) {
			if(openStackSwiftUrl.startsWith("/")) {
				return "http://%s:%s%s%s";
			} else {
				return "http://%s:%s/%s%s";
			}
		} else {
			if(openStackSwiftUrl.startsWith("/")) {
				return "http://%s:%s%s/%s";
			} else {
				return "http://%s:%s/%s/%s";
			}
		}
	}
	
	private String makeSwiftObjectUrl() {
		String openStackSwiftUrl = getConfigValue("openStackSwiftUrl");
		if(openStackSwiftUrl.endsWith("/")) {
			if(openStackSwiftUrl.startsWith("/")) {
				return "http://%s:%s%s%s/%s";
			} else {
				return "http://%s:%s/%s%s/%s";
			}
		} else {
			if(openStackSwiftUrl.startsWith("/")) {
				return "http://%s:%s%s/%s/%s";
			} else {
				return "http://%s:%s/%s/%s/%s";
			}
		}
	}
	
	/**
	 * PUT SwiftUrl/v1/{account}/{container}
	 * related config key: openStackSwiftHost	openStackSwiftPort openStackSwiftUrl	openStackSwiftProtocol
	 * 
	 * @throws IllegalArgumentException if input name is empty
	 * @return
	 */
	SwiftRestAPIResult createContainer(String name) {
		if(null == name || "".equals(name)) {
			throw new IllegalArgumentException("name is required.");
		}
		if(authToken == null) {
			if(login() == false) {
				logger.severe("Relogin failed");
				return new SwiftRestAPIResult(false, "Relogin Failed");
			}
		}
		try {
			// TODO: config check
			String openStackSwiftHost = getConfigValue("openStackSwiftHost");
			String openStackSwiftPort = getConfigValue("openStackSwiftPort");
			String openStackSwiftUrl = getConfigValue("openStackSwiftUrl");
			String urlTemplate = makeSwiftContainerUrl();
			String containerUrl = String.format(urlTemplate, openStackSwiftHost, openStackSwiftPort, openStackSwiftUrl, name);
			URL url = new URL(containerUrl);
			HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			conn.setDoOutput(true);
			conn.setRequestMethod("PUT");
			conn.setRequestProperty(SWIFT_AUTHHEADER_NAME, authToken);
			
			if(conn.getResponseCode() == HttpURLConnection.HTTP_CREATED) {
				logger.info("folder created");
				conn.disconnect();
				return new SwiftRestAPIResult(true, "Create Folder Success");
			} else if(conn.getResponseCode() == HttpURLConnection.HTTP_ACCEPTED) {
				logger.info("folder exists");
				conn.disconnect();
				return new SwiftRestAPIResult(true, "Folder Exist");
			} else {
				conn.disconnect();
				logger.warning("Create Folder Failed");
				return new SwiftRestAPIResult(false, "Create Folder Failed");
			}
		} catch (MalformedURLException e) {
			logger.log(Level.SEVERE, e.getMessage(), e);
		} catch (IOException e) {
			logger.log(Level.SEVERE, e.getMessage(), e);
		}
		return null;
	}
	
	/**
	 * Create Object
	 * related config key: openStackSwiftHost	openStackSwiftPort	openStackSwiftProtocol
	 * @param fileToSwift
	 * @return
	 */
	public SwiftCreateObjectResult putObject(File fileToSwift) {
		if(null == fileToSwift || !fileToSwift.exists() || !fileToSwift.canRead()) {
			throw new IllegalArgumentException("fileToSwift must be required, exists and readable.");
		}
		if(authToken == null) {
			if(login() == false) {
				logger.severe("Relogin failed");
				return new SwiftCreateObjectResult(false, "Relogin Failed");
			}
		}
		try {
			String containerName = getContainerName();
			String objectId = UUID.randomUUID().toString();
			// TODO: config check
			String openStackSwiftHost = getConfigValue("openStackSwiftHost");
			String openStackSwiftPort = getConfigValue("openStackSwiftPort");
			String openStackSwiftUrl = getConfigValue("openStackSwiftUrl");
			String urlTemplate = makeSwiftObjectUrl();
			
			//String objectUrl = String.format("http://10.120.137.149:8080/v1/AUTH_45d4fac68382419dbd1db6c44ecff98e/samchiu/%s", objectId);
			String objectUrl = String.format(urlTemplate, openStackSwiftHost, openStackSwiftPort, openStackSwiftUrl, containerName, objectId);
			URL url = new URL(objectUrl);
			HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			conn.setDoOutput(true);
			conn.setRequestMethod("PUT");
			conn.setRequestProperty(SWIFT_AUTHHEADER_NAME, authToken);
			byte[] data = Files.readAllBytes( fileToSwift.toPath() );
			OutputStream output = conn.getOutputStream();
			output.write(data);
			output.flush();
			
			if(conn.getResponseCode() == HttpURLConnection.HTTP_CREATED) {
				logger.info("object created");
				conn.disconnect();
				SwiftCreateObjectResult swiftCreateObjectResult = new SwiftCreateObjectResult(true, "Create object Success");
				swiftCreateObjectResult.objectId = objectId;
				swiftCreateObjectResult.objectUrl = objectUrl;
				return swiftCreateObjectResult;
			} else if(conn.getResponseCode() == HttpURLConnection.HTTP_ACCEPTED) {
				logger.info("object exists");
				conn.disconnect();
				return new SwiftCreateObjectResult(true, "object Exist");
			} else if(conn.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
				// container does not exist
				logger.warning("Could be container does not exist, trying create container then recreate object");
				if(createContainer(containerName).status == true) {
					conn.disconnect();
					if(postObject(fileToSwift, objectUrl).status == true) {
						logger.info("object created");
						SwiftCreateObjectResult swiftCreateObjectResult = new SwiftCreateObjectResult(true, "Create object Success");
						swiftCreateObjectResult.objectId = objectId;
						swiftCreateObjectResult.objectUrl = objectUrl;
						return swiftCreateObjectResult;
					} else {
						logger.info("retry object create failed");
						logger.warning("Create object retry Failed");
						return new SwiftCreateObjectResult(false, "Create object Failed");
					}
				} else {
					logger.warning("Create Container Failed");
					conn.disconnect();
					logger.warning("Create object retry Failed");
					return new SwiftCreateObjectResult(false, "Create object Failed: cannot create container");
				}
			} else {
				conn.disconnect();
				logger.warning("Create object Failed");
				return new SwiftCreateObjectResult(false, "Create object Failed");
			}
		} catch (MalformedURLException e) {
			logger.log(Level.SEVERE, e.getMessage(), e);
		} catch (IOException e) {
			logger.log(Level.SEVERE, e.getMessage(), e);
		}
		return null;
	}
	
	/**
	 * Update Object
	 * related config key: openStackSwiftHost	openStackSwiftPort	openStackSwiftProtocol
	 * @param fileToSwift
	 * @return
	 */
	public SwiftRestAPIResult postObject(File fileToSwift, String objectUrl) {
		if(null == fileToSwift || !fileToSwift.exists() || !fileToSwift.canRead()) {
			throw new IllegalArgumentException("fileToSwift must be required, exists and readable.");
		}
		if(authToken == null) {
			if(login() == false) {
				logger.severe("Relogin failed");
				return new SwiftCreateObjectResult(false, "Relogin Failed");
			}
		}
		try {
			URL url = new URL(objectUrl);
			HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			conn.setDoOutput(true);
			conn.setRequestMethod("PUT");
			conn.setRequestProperty(SWIFT_AUTHHEADER_NAME, authToken);
			byte[] data = Files.readAllBytes( fileToSwift.toPath() );
			OutputStream output = conn.getOutputStream();
			output.write(data);
			output.flush();
			
			if(conn.getResponseCode() == HttpURLConnection.HTTP_CREATED) {
				logger.info("object updated");
				conn.disconnect();
				return new SwiftCreateObjectResult(true, "Update object Success");
			} else {
				conn.disconnect();
				logger.warning("Update object Failed");
				return new SwiftCreateObjectResult(false, "Update object Failed");
			}
		} catch (MalformedURLException e) {
			logger.log(Level.SEVERE, e.getMessage(), e);
		} catch (IOException e) {
			logger.log(Level.SEVERE, e.getMessage(), e);
		}
		return null;
	}
	
	/**
	 * 
	 * @param objectUrl
	 * @return
	 */
	public byte[] getObject(String objectUrl) {
		if(null == objectUrl || "".equals(objectUrl)) {
			logger.warning("objectUrl is empty");
			return null;
		}
		if(authToken == null) {
			if(login() == false) {
				logger.severe("Relogin failed");
				return null;
			}
		}
		HttpURLConnection conn = null;
		try {
			URL url = new URL(objectUrl);
			conn = (HttpURLConnection) url.openConnection();
			conn.setDoOutput(true);
			conn.setRequestMethod("GET");
			conn.setRequestProperty(SWIFT_AUTHHEADER_NAME, authToken);
			
			if(conn.getResponseCode() == HttpURLConnection.HTTP_OK) {
				// 雖然有spring framework有可能沒有這個class
				// return StreamUtils.copyToByteArray(conn.getInputStream());
				InputStream in = conn.getInputStream();
				ByteArrayOutputStream buffer = new ByteArrayOutputStream();
				int nRead;
				byte[] data = new byte[16384];
				while((nRead = in.read(data, 0, data.length)) != -1) {
					buffer.write(data, 0, nRead);
				}
				buffer.flush();
				return buffer.toByteArray();
			} else {
				logger.warning("get object Failed: " + conn.getResponseCode());
				return null;
			}
		} catch (MalformedURLException e) {
			logger.log(Level.SEVERE, e.getMessage(), e);
		} catch (IOException e) {
			logger.log(Level.SEVERE, e.getMessage(), e);
		} finally {
			if(conn != null) {
				conn.disconnect();
			}
		}
		return null;
	}
	
	/*
	 * 以下三個API有但不確定會不會用到先保留不實作
	 * 第一個method跟Dennis詢問stream播放有關，因為官方API有支援HTTP Range Request可以用在某些播放器或HTML5 video tag
	public byte[] getObject(String objectUrl, String rangeCommand) {
		return null;
	}
	
	public Map<String, String> headObject(String objectUrl) {
		return null;
	}
	
	public void postObjectMetadata(String objectUrl, Map<String, String> metadatas) {
		
	}
	*/
	
	public class SwiftRestAPIResult {
		SwiftRestAPIResult() {}
		
		SwiftRestAPIResult(boolean status, String message) {
			this.status = status;
			this.message = message;
		}
		
		/**
		 * Operation status. true: success, false: failed
		 */
		public boolean status;
		/**
		 * Operation message.
		 */
		public String message;
	}
	
	public class SwiftCreateObjectResult extends SwiftRestAPIResult {
		SwiftCreateObjectResult() {}
		
		SwiftCreateObjectResult(boolean status, String message) {
			super(status, message);
		}
		
		/**
		 * Object Id (object name in swift)
		 * only putObject()
		 */
		public String objectId;
		/**
		 * Object full url.
		 * only putObject()
		 */
		public String objectUrl;
	}
	
	
	/**
	 * FIXME: remove me this is for test
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		StorageConfig config = new StorageConfig();
		Map<String, Object> parameter = new java.util.HashMap<>();
		parameter.put("openStackLoginHost", "10.120.137.149");
		parameter.put("openStackLoginPort", 5000);
		parameter.put("openStackLoginUrl", "v3/auth/tokens");
		parameter.put("account", "theuser");
		parameter.put("password", "thepassword");
		parameter.put("swiftProject", "jarvis");
		parameter.put("openStackSwiftHost", "10.120.137.149");
		parameter.put("openStackSwiftPort", 8080);
		parameter.put("openStackSwiftUrl", "v1/AUTH_45d4fac68382419dbd1db6c44ecff98e/");
		config.setParameters(parameter);
		OpenStackConnector connector = new OpenStackConnector(config);
		OpenStackStorage storage = new OpenStackStorage(connector);
		if(storage.login()) {
			storage.createContainer("test2");
			SwiftCreateObjectResult result = storage.putObject(new File("D:\\w\\Untitled.png"));
			System.out.println(result.objectUrl);
			System.out.println(result.objectId);
			
			SwiftRestAPIResult r = storage.postObject(new File("D:\\w\\dijkstra.png"), result.objectUrl);
			System.out.println(r.status);
			System.out.println(r.message);
			
			byte[] target = storage.getObject(result.objectUrl);
			byte[] source = Files.readAllBytes( new File("D:\\w\\dijkstra.png").toPath() );
//			if(org.springframework.util.DigestUtils.md5DigestAsHex(target).equals(org.springframework.util.DigestUtils.md5DigestAsHex(source))) {
//				System.out.println("Object is identical");
//			} else {
//				System.out.println("Big Trouble");
//			}
			if(target.length == source.length) {
				for(int i=0; i<target.length; i++) {
					if(target[i] != source[i]) {
						System.out.println("Big Trouble");
					}
				}
			} else {
				System.out.println("Big Trouble");
			}
		}
	}
}
