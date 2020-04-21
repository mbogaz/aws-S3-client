package main;


import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.conn.ssl.X509HostnameVerifier;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;

public class SGSocketFactory extends SSLSocketFactory {

    private SSLContext sslContext;

    public SGSocketFactory(SSLContext context, X509HostnameVerifier verifier) {
        super(context, verifier);
        sslContext = context;
    }

    @Override
    public Socket createSocket(Socket socket, String host, int port,
                               boolean autoClose) throws IOException, UnknownHostException {
        return sslContext.getSocketFactory().createSocket(socket, host, port,
                autoClose);
    }

    @Override
    public Socket createSocket() throws IOException {
        return sslContext.getSocketFactory().createSocket();
    }
}

