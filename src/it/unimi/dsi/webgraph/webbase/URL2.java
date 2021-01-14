package it.unimi.dsi.webgraph.webbase;


import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;


/**
 * A reimplementation of URL better tailored to our needs.
 *
 * This class performs some normalization on URL names, etc. In particular,
 * it strips references.
 */

public final class URL2 implements java.io.Serializable, Comparable<URL2> {

	 static final long serialVersionUID = 2163820562971252003L;

    /** The protocol to use (ftp, http, nntp, etc.). */
    private String protocol;

    /** The host name in which to connect to. */
    private String host;

    /** The protocol port to connect to. */
    private int port = -1;

    /** The specified file name on that host (i.e., the path without the # reference). */
    private String file;

    /** The query part of this URL. */
    private transient String query;

    /** The authority part of this URL (i.e., username:passwd@host:port). */
    private String authority;

    /** The path part of this URL (everything after the host name). */
    private transient String path;

    /** The userinfo part of this URL. */
    private transient String userInfo;

	 /** Our string representation. */
	 private String stringRepr;

    /** Our hash code. */
    private int hashCode = -1;

    /** Our 64 bit CRC. */
    private long hashCode64 = -1;


    /** A string to get quickly hexadecimal digits. */
    private final static char[] HEX_DIGIT = "0123456789ABCDEF".toCharArray();

    /** Normalizes a URL fragment.
     *
     * <P>This method return the normalization of its argument. All character that
     * are illegal are first UTF-8 encoded, and then represented with the %-notation.
     *
     * @param UTF8Encoder a (possibly cached) UTF-8 encoder.
     * @param fragment a URL fragment (possibly <code>null</code>).
     * @return the normalized version.
     */


    public static String normalizeURLFragment(final CharsetEncoder UTF8Encoder, final String fragment) throws CharacterCodingException {

		if (fragment == null) return null;

		/* First of all, if there are no non-ASCII characters at all we just return the string. */
		int i = fragment.length();
		while(i-- != 0) if (fragment.charAt(i) > 127) break;
		if (i < 0) return fragment;

		/* Otherwise, we pass everything in UTF-8 e substitute characters beyond 127 with the %-notation. */
		ByteBuffer bb = UTF8Encoder.encode(CharBuffer.wrap(fragment));
		StringBuffer t = new StringBuffer();
		final byte b[] = bb.array();
		final int l = bb.limit();

		for(i = 0; i < l; i++) {
			if (b[i] < 0) {
				t.append('%');
				t.append(HEX_DIGIT[(b[i] & 0xFF) / 16]);
				t.append(HEX_DIGIT[(b[i] & 0xFF) % 16]);
			}
			else t.append((char)b[i]);
		}

		return t.toString();
    }

    /** Normalizes a URL fragment.
     *
     * <P>This method return the normalization of its argument. All character that
     * are illegal are first UTF-8 encoded, and then represented with the %-notation.
     *
     * @param fragment a URL fragment (possibly <code>null</code>).
     * @return the normalized version.
     */

    public static String normalizeURLFragment(final String fragment) throws CharacterCodingException {
		return normalizeURLFragment(Charset.forName("UTF-8").newEncoder(), fragment);
    }


    /**
     * Creates a URL from the specified <code>protocol</code>
     * name, <code>host</code> name, and <code>file</code> name. The
     * default port for the specified protocol is used.
     * <p>
     * This method is equivalent to calling the four-argument
     * constructor with the arguments being <code>protocol</code>,
     * <code>host</code>, <code>-1</code>, and <code>file</code>.
     *
     * @param      protocol   the name of the protocol to use.
     * @param      host       the name of the host.
     * @param      file       the file on the host.
     */
	 /*public URL2(String protocol, String host, String file) {
		  this(protocol, host, -1, file);
		  }*/

    /**
     * Creates a <code>URL</code> object from the specified
     * <code>protocol</code>, <code>host</code>, <code>port</code>
     * number and <code>file</code>. Specifying
     * a <code>port</code> number of <code>-1</code> indicates that
     * the URL should use the default port for the protocol.
     *
     * @param      protocol   the name of the protocol to use.
     * @param      host       the name of the host.
     * @param      port       the port number on the host.
     * @param      file       the file on the host
     */
	 /*    public URL2(String protocol, String host, int port, String file) {
		  this.protocol = protocol;
		  host = host.toLowerCase();
		  this.host = host;
		  int ind = file.indexOf('#');
		  this.file = ind < 0 ? file: file.substring(0, ind);
		  // No refs
		  //this.ref = ind < 0 ? null: file.substring(ind + 1);
        int q = file.lastIndexOf('?');
        if (q != -1) {
            query = file.substring(q+1);
            path = file.substring(0, q);
        } else
            path = file;
		  // Note: we don't do validation of the URL here. Too risky to change
		  // right now, but worth considering for future reference. -br
        this.port = port;
        if (host != null && host.length() > 0) {
            authority = (port == -1) ? host : host + ":" + port;
        }

		  }*/

    /**
     * Creates a <code>URL</code> object from the <code>String</code>
     * representation.
     * <p>
     * This constructor is equivalent to a call to the two-argument
     * constructor with a <code>null</code> first argument.
     *
     * @param      spec   the <code>String</code> to parse as a URL.
     */
    public URL2(String spec) {
		  this(null, spec);
    }

    /**
     * Creates a URL by parsing the given spec within a specified context.
     *
     * The new URL is created from the given context URL and the spec
     * argument as described in RFC2396 &quot;Uniform Resource Identifiers : Generic
     * Syntax&quot; :
     * <blockquote><pre>
     *          &lt;scheme&gt;://&lt;authority&gt;&lt;path&gt;?&lt;query&gt;#&lt;fragment&gt;
     * </pre></blockquote>
     * The reference is parsed into the scheme, authority, path, query and
     * fragment parts. If the path component is empty and the scheme,
     * authority, and query components are undefined, then the new URL is a
     * reference to the current document. Otherwise the any fragment and query
     * parts present in the spec are used in the new URL.
     *
     * If the scheme component is defined in the given spec and does not match
     * the scheme of the context, then the new URL is created as an absolute
     * URL based on the spec alone. Otherwise the scheme component is inherited
     * from the context URL.
     *
     * If the authority component is present in the spec then the spec is
     * treated as absolute and the spec authority and path will replace the
     * context authority and path. If the authority component is absent in the
     * spec then the authority of the new URL will be inherited from the
     * context.
     *
     * If the spec's path component begins with a slash character &quot;/&quot; then the
     * path is treated as absolute and the spec path replaces the context path.
     * Otherwise the path is treated as a relative path and is appended to the
     * context path. The path is canonicalized through the removal of directory
     * changes made by occurences of &quot;..&quot; and &quot;.&quot;.
     *
     * For a more detailed description of URL parsing, refer to RFC2396.
	  *
	  * NOTE: some sanitization is now performed on paths and queries. In particular,
	  * "//" sequences are collapsed in paths, and "/" is %-encoded in queries.
     *
     * @param      context   the context in which to parse the specification.
     * @param      spec      the <code>String</code> to parse as a URL.
     */
    public URL2(URL2 context, String spec)  {
		  int i, limit, c;
		  int start = 0;
		  String newProtocol = null;
		  boolean aRef=false;

		  /* We can tolerate spaces in specs, but not tabs or newlines. */
		  if (spec.indexOf('\t') >= 0 || spec.indexOf('\n') >= 0 || spec.indexOf('\r') >= 0) return;

		  limit = spec.length();
		  while ((limit > 0) && (spec.charAt(limit - 1) <= ' ')) {
				limit--;	//eliminate trailing whitespace
		  }
		  while ((start < limit) && (spec.charAt(start) <= ' ')) {
				start++;	// eliminate leading whitespace
		  }

		  if (spec.regionMatches(true, start, "url:", 0, 4)) {
				start += 4;
		  }
		  if (start < spec.length() && spec.charAt(start) == '#') {
				/* we're assuming this is a ref relative to the context URL.
				 * This means protocols cannot start w/ '#', but we must parse
				 * ref URL's like: "hello:there" w/ a ':' in them.
				 */
				aRef=true;
		  }
		  for (i = start ; !aRef && (i < limit) &&
					  ((c = spec.charAt(i)) != '/') ; i++) {
				if (c == ':') {
					 String s = spec.substring(start, i).toLowerCase();
					 if (isValidProtocol(s)) {
						  newProtocol = s;
						  start = i + 1;
					 }
					 break;
				}
		  }

		  // Only use our context if the protocols match.
		  protocol = newProtocol;
		  if ((context != null) && ((newProtocol == null) ||
											 newProtocol.equalsIgnoreCase(context.protocol))) {

				// If the context is a hierarchical URL scheme and the spec
				// contains a matching scheme then maintain backwards
				// compatibility and treat it as if the spec didn't contain
				// the scheme; see 5.2.3 of RFC2396
				if (context.path != null && context.path.startsWith("/"))
					 newProtocol = null;

				if (newProtocol == null) {
					 protocol = context.protocol;
					 authority = context.authority;
					 userInfo = context.userInfo;
					 host = context.host;
					 port = context.port;
					 file = context.file;
				}
		  }

		  i = spec.indexOf('#', start);
		  if (i >= 0) {
				// No refs
				//ref = spec.substring(i + 1, limit);
				limit = i;

				while ((start < limit) && (spec.charAt(limit - 1) <= ' ')) {
					 limit--;	// Eliminate trailing whitespace *after fragment*.
				}
		  }

		  parseURL(this, spec, start, limit);

	 }

    /**
     * Parses the string representation of a <code>URL</code> into a
     * <code>URL</code> object.
     * <p>
     * If there is any inherited context, then it has already been
     * copied into the <code>URL</code> argument.
     * <p>
     * The <code>parseURL</code> method of <code>URLStreamHandler</code>
     * parses the string representation as if it were an
     * <code>http</code> specification. Most URL protocol families have a
     * similar parsing. A stream protocol handler for a protocol that has
     * a different syntax must override this routine.
     *
     * @param   u       the <code>URL</code> to receive the result of parsing
     *                  the spec.
     * @param   spec    the <code>String</code> representing the URL that
     *                  must be parsed.
     * @param   start   the character index at which to begin parsing. This is
     *                  just past the '<code>:</code>' (if there is one) that
     *                  specifies the determination of the protocol name.
     * @param   limit   the character position to stop parsing at. This is the
     *                  end of the string or the position of the
     *                  "<code>#</code>" character, if present. All information
     *                  after the sharp sign indicates an anchor.
     */
    protected void parseURL(URL2 u, String spec, int start, int limit) {
        // These fields may receive context content if this was relative URL
        String authority = u.getAuthority();
        String userInfo = u.getUserInfo();
        String host = u.getHost();
        int port = u.getPort();
        String file = u.getFile();

        // This field has already been parsed
        String ref = u.getRef();

        // These fields will not inherit context content
        String query = null;

		  boolean isRelPath = false;
		  boolean queryOnly = false;

		  // FIX: should not assume query if opaque
        // Strip off the query part
		  if (start < limit) {
            int queryStart = spec.indexOf('?');
            queryOnly = queryStart == start;
            if (queryStart != -1 && queryStart+1 < limit) {
                query = spec.substring(queryStart+1, limit);
                if (limit > queryStart)
                    limit = queryStart;
                spec = spec.substring(0, queryStart);
            }
		  }

		  int i = 0;
        // Parse the authority part if any
		  if ((start <= limit - 2) && (spec.charAt(start) == '/') &&
				(spec.charAt(start + 1) == '/')) {
				start += 2;
				i = spec.indexOf('/', start);
            if (i < 0) {
					 i = spec.indexOf('?', start);
					 if (i < 0)
                    i = limit;
				}

            authority = spec.substring(start, i);

            int ind = authority.indexOf('@');
            if (ind != -1) {
                userInfo = authority.substring(0, ind);
                host = authority.substring(ind+1).toLowerCase();
					 authority = userInfo + "@" + host;
				}
				else authority = host = authority.toLowerCase();

            ind = host.indexOf(':');
				port = -1;
				if (ind >= 0) {
					 // port can be null according to RFC2396
					 if (host.length() > (ind + 1)) {
						  try {
								port = Integer.parseInt(host.substring(ind + 1));
						  }
						  catch(NumberFormatException e) {
								port = -1;
						  }
					 }
					 host = host.substring(0, ind);
				}

				start = i;
				// If the authority is defined then the path is defined by the
            // spec only; See RFC 2396 Section 5.2.4.
            if (authority != null && authority.length() > 0)
                file = "";
		  }
        if (host == null) {
				host = "";
		  }

        // Parse the file path if any
		  if (start < limit) {
				if (spec.charAt(start) == '/') {
					 file = spec.substring(start, limit);
				} else if (file != null && file.length() > 0) {
					 isRelPath = true;
					 int ind = file.lastIndexOf('/');
					 String seperator = "";
					 if (ind == -1 && authority != null)
						  seperator = "/";
					 file = file.substring(0, ind + 1) + seperator +
						  spec.substring(start, limit);

				} else {
					 String seperator = (authority != null) ? "/" : "";
					 file = seperator + spec.substring(start, limit);
				}
		  } else if (queryOnly && file != null) {
            int ind = file.lastIndexOf('/');
            if (ind < 0)
                ind = 0;
            file = file.substring(0, ind) + "/";
        }
		  if (file == null)
				file = "";

		  if (isRelPath) {
            // Remove embedded /./
            while ((i = file.indexOf("/./")) >= 0) {
					 file = file.substring(0, i) + file.substring(i + 2);
				}
            // Remove embedded /../
				while ((i = file.indexOf("/../")) >= 0) {
					 if ((limit = file.lastIndexOf('/', i - 1)) >= 0) {
						  file = file.substring(0, limit) + file.substring(i + 3);
					 } else {
						  file = file.substring(i + 3);
					 }
				}
            // Remove trailing ..
            while (file.endsWith("/..")) {
                i = file.indexOf("/..");
					 if ((limit = file.lastIndexOf('/', i - 1)) >= 0) {
						  file = file.substring(0, limit+1);
					 } else {
						  file = file.substring(0, i);
					 }
				}
            // Remove trailing .
            if (file.endsWith("/."))
                file = file.substring(0, file.length() -1);
		  }


		  if (file.equals("")) file = "/";

		  // "//" sanitization.

		  while ((i = file.indexOf("//")) >= 0) {
				file = file.substring(0, i) + file.substring(i + 1);
		  }


		  // Forced escaping of "/" in queries

		  if (query != null) {
				while ((i = query.indexOf('/')) >= 0) {
					 query = query.substring(0, i) + "%2F" + query.substring(i + 1);
				}
		  }

		  /* Normalization */

		  try {
				query = normalizeURLFragment(query);
				file = normalizeURLFragment(file);
				ref = normalizeURLFragment(ref);
		  }
		  catch(CharacterCodingException nonValid) {
				protocol = null;
				host = null;
		  }


		  // If host/authority end in ".", remove it.

		  if (host != null && host.endsWith(".")) host = host.substring(0, host.length()-1);
		  if (authority != null && authority.endsWith(".")) authority = authority.substring(0, authority.length()-1);

		  // Why not protocol instead of u.getProtocol()?
		  // Note that we pass "file" as actual parameter of the formal parameter "path"
		  set(u.getProtocol(), host, port, authority, userInfo, file, query, ref);
    }


    /**
     * Sets the specified 8 fields of the URL. This is not a public method so
     * that only URLStreamHandlers can modify URL fields. URLs are otherwise
     * constant.
     *
     * @param protocol the name of the protocol to use
     * @param host the name of the host
     * @param port the port number on the host
     * @param authority the authority part for the url
     * @param userInfo the username and password
     * @param path the file on the host
     * @param ref the internal reference in the URL
     * @param query the query part of this URL
     */
    protected void set(String protocol, String host, int port,
                       String authority, String userInfo, String path,
                       String query, String ref) {
          synchronized (this) {
				this.protocol = protocol;
				this.host = host;
				this.port = port;
				this.file = query == null ? path : path + "?" + query;
            this.userInfo = userInfo;
            this.path = path;
				//this.ref = ref; No refs
				/* This is very important. We must recompute this after the
				 * URL has been changed. */
            this.query = query;
            this.authority = authority;
				this.hashCode = -1;
		  }
    }


	 /*
	  * Returns true if specified string is a valid protocol name.
			*/
	 private boolean isValidProtocol(String protocol) {
		  int len = protocol.length();
		  if (len < 1)
				return false;
		  char c = protocol.charAt(0);
		  if (!Character.isLetter(c))
				return false;
		  for (int i = 1; i < len; i++) {
				c = protocol.charAt(i);
				if (!Character.isLetterOrDigit(c) && c != '.' && c != '+' &&
					 c != '-') {
					 return false;
				}
		  }
		  return true;
	 }


	 /*
	  * Returns <code>true</code> if this URL is sensible. This includes
	  * having a nonempty protocol, and no \r\n\t or in the string.
	  *
	  * @return <code>true</code> is this URL is sensible.
	  */
	 public boolean isValid() {
		  return protocol != null && host != null;
	 }

	 /**
	  * Returns the query part of this <code>URL</code>.
	  *
	  * @return  the query part of this <code>URL</code>.
	  */
	 public String getQuery() {
		  return query;
	 }

	 /**
			* Returns the path part of this <code>URL</code>.
			*
			* @return  the path part of this <code>URL</code>.
			*/
	 public String getPath() {
		  return path;
	 }

	 /**
			* Returns the userInfo part of this <code>URL</code>.
			*
			* @return  the userInfo part of this <code>URL</code>.
			*/
	 public String getUserInfo() {
		  return userInfo;
	 }

	 /**
			* Returns the authority part of this <code>URL</code>.
			*
			* @return  the authority part of this <code>URL</code>.
			*/
	 public String getAuthority() {
		  String host = getHost();
		  if (host == null) host = "";

		  String res =  userInfo == null || userInfo.equals("") ? host : userInfo + "@" + host;
		  if (port != -1 && port != 80) res += ":" + port;

		  return res;
	 }

	 /**
			* Returns the port number of this <code>URL</code>.
			* Returns -1 if the port is not set.
			*
			* @return  the port number
			*/
	 public int getPort() {
		  return port;
	 }

	 /**
			* Returns the protocol name of this <code>URL</code>.
			*
			* @return  the protocol of this <code>URL</code>.
			*/
	 public String getProtocol() {
		  return protocol;
	 }

	 /**
	  * An alias for {@link #getProtocol()}.
	  *
	  * @return  the protocol of this <code>URL</code>.
	  */
	 public String getScheme() {
		  return protocol;
	 }

	 /**
			* Returns the host name of this <code>URL</code>, if applicable.
			*
			* @return  the host name of this <code>URL</code>.
			*/
	 public String getHost() {
		  return host;
	 }

	 /**
			* Returns the file name of this <code>URL</code>.
			*
			* @return  the file name of this <code>URL</code>.
			*/
	 public String getFile() {
		  return file;
	 }

	/**
	 * Returns the file name extension of this <code>URL</code>.
	 * In case of file name is <code>index.html</code>,
	 * <code>html</code> will be returned but if no valid extension
	 * is found <code>null</code> will be returned.
	 *
	 * @return  the file name extension of this <code>URL</code>.
	 */
	 public String getFileExtension() {
		  String extension = file;
		  int i;
		  boolean valid = false;

		  if (!extension.endsWith("/")) {
				if ((i = extension.indexOf('?')) > 0) extension = extension.substring(0, i);
				if ((i = extension.lastIndexOf('.')) > 0) {
					 extension = extension.substring(i + 1, extension.length());
					 if (extension.length() != 0 && extension.length() < 6) valid = true;
				}
		  }
		  if (valid) return extension.toLowerCase();
		  else return null;
	 }

	 /**
			* Returns the anchor (also known as the "reference") of this
			* <code>URL</code>.
			*
			* @return  the anchor (also known as the "reference") of this
			*          <code>URL</code>.
			*/
	 public String getRef() {
		  return null; // ref; No refs
	 }

	 /**
	  * An alias for {@link #getRef()}.
	  *
	  * @return  the anchor (also known as the "reference") of this
	  *          <code>URL</code>.
	  */
	 public String getFragment() {
		  return null; // ref; No refs
	 }

	 /**
			* Compares two URLs.  The result is <code>true</code> if and
			* only if the argument is not <code>null</code> and is a
			* <code>URL</code> object that represents the same
			* <code>URL</code> as this object. Two URL objects are equal if
			* they have the same protocol and reference the same host, the
			* same port number on the host, and the same file and anchor on
			* the host.
			*
			* @param   obj   the URL to compare against.
			* @return  <code>true</code> if the objects are the same;
			*          <code>false</code> otherwise.
			*/
	 @Override
	public boolean equals(Object obj) {
		  if (!(obj instanceof URL2))
				return false;

		  URL2 url = (URL2)obj;

		  return
				(protocol == url.protocol || protocol != null && protocol.equals(url.protocol)) &&
				(userInfo == url.userInfo || userInfo != null && userInfo.equals(url.userInfo)) &&
				(host == url.host || host != null && host.equals(url.host)) &&
				(port == -1 ? 80 : port) == (url.port == -1 ? 80 : url.port) &&
				(file == url.file || file != null && file.equals(url.file));
	 }

	 @SuppressWarnings("unchecked")
	private static int compareValues(@SuppressWarnings("rawtypes") Comparable o1, @SuppressWarnings("rawtypes") Comparable o2) {
		  if (o1 == o2) return 0;
		  if (o1 == null) return -1;
		  if (o2 == null) return 1;
		  return o1.compareTo(o2);
	 }

	 @Override
	public int compareTo(URL2 url) {
		  int r;
		  if ((r = compareValues(protocol, url.protocol)) != 0) return r;
		  if ((r = compareValues(userInfo, url.userInfo)) != 0) return r;
		  if ((r = compareValues(getHost(), url.getHost())) != 0) return r;
		  if ((r = compareValues(file, url.file)) != 0) return r;
		  return (port == -1 ? 80 : port) - (url.port == -1? 80 : url.port);
	 }

	 @Override
	public int hashCode() {
		  if (hashCode != -1) return hashCode;
		  return hashCode = toString().hashCode();
	 }

	 public long hashCode64() {
		  if (hashCode64 != -1) return hashCode64;
		  return hashCode64 = CRC64.compute(toString());
	 }

	 /**
	  * Constructs a string representation of this <code>URL</code>.
	  *
	  * @return  a string representation of this object.
	  */
	 @Override
	public String toString() {

		  if (stringRepr != null) return stringRepr;

		  StringBuffer result = new StringBuffer(protocol != null ? protocol : "");
        result.append(':');
        if (getAuthority() != null && getAuthority().length() > 0) {
            result.append("//");
            result.append(getAuthority());
        }
        if (getFile() != null) {
            result.append(getFile());
        }
		  /* ref is always null.

		  if (addRef && getRef() != null) {
		  result.append("#");
		  result.append(getRef());
		  }*/
		  return stringRepr = result.toString();
	 }

	 /** Extracts domain name for a given URL. <em>Very useful to avoid correlated-links</em>.
	  * This method works by considering the right-most, most-significant
	  * and non-common suffix of a given URL.
	  * Examples:
	  * <P><code>http://www.ox.ac.uk/</code> returns: <code>ox.ac.uk</code>
	  * <code>http://something.somethingelse.web.com/</code> returns: <code>somethingelse.web.com</code>
	  * <code>http://www.microsoft.com/</code> returns: <code>microsoft.com</code>
	  * <code>http://www.dsi.unimi.it/</code> returns: <code>unimi.it</code>
	  *
	  * @return a {@link String} indicating the domain name.
	  */
	 public String getDomain() {
		  /*
		  // This is a list of tokens we can find in many TLDs
		  // Example: http://www.ox.ac.uk/ <= ac.uk must be treated
		  // as if it was a domain extension!
		  ObjectOpenHashSet commonSuffixes = new ObjectOpenHashSet();
		  commonSuffixes.add("co");
		  commonSuffixes.add("ac");
		  commonSuffixes.add("web");
		  commonSuffixes.add("com");
		  commonSuffixes.add("gov");
		  commonSuffixes.add("net");
		  commonSuffixes.add("org");
		  commonSuffixes.add("edu");
		  commonSuffixes.add("mil");
		  commonSuffixes.add("biz");

		  String tok[] = new String[3];
		  for (int i = 0; i < 3; i++) tok[i] = null;
		  StringTokenizer st = new StringTokenizer(host, ".");
		  while (st.hasMoreTokens()) {
		  tok[0] = tok[1]; tok[1] = tok[2];
		  tok[2] = st.nextToken();
		  }
		  if (tok[0] != null && tok[1] != null && tok[2] != null && commonSuffixes.contains(tok[1])) host = tok[0] + "." + tok[1] + "." + tok[2];
		  else if (tok[1] != null && tok[2] != null) host = tok[1] + "." + tok[2];
		  else if (tok[2] != null) host = tok[2];
		  else host = new String();
		  */

		  return host;
	 }

    private void readObject(java.io.ObjectInputStream s)
		  throws IOException, ClassNotFoundException
    {
		  s.defaultReadObject();	// read the fields

        // Construct authority part
        if (authority == null &&
				((host != null && host.length() > 0) || port != -1)) {
				if (host == null)
					 host = "";
            authority = (port == -1) ? host : host + ":" + port;

            // Handle hosts with userInfo in them
            int at = host.lastIndexOf('@');
            if (at != -1) {
                userInfo = host.substring(0, at);
                host = host.substring(at+1);
				}
        } else if (authority != null) {
            // Construct user info part
            int ind = authority.indexOf('@');
            if (ind != -1)
                userInfo = authority.substring(0, ind);
		  }

        // Construct path and query part
        path = null;
        query = null;
        if (file != null) {
				// Fix: only do this if hierarchical?
            int q = file.lastIndexOf('?');
            if (q != -1) {
                query = file.substring(q+1);
                path = file.substring(0, q);
            } else
                path = file;
        }
    }
}
