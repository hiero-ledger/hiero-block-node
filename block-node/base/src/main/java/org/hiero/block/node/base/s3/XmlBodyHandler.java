// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.base.s3;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.http.HttpResponse;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

/**
 * A custom {@link HttpResponse.BodyHandler} that parses the response body as XML.
 */
public class XmlBodyHandler implements HttpResponse.BodyHandler<Document> {

    /**
     * {@inheritDoc}
     *
     * @throws UncheckedIOException if an error occurs during parsing
     */
    @Override
    public HttpResponse.BodySubscriber<Document> apply(HttpResponse.ResponseInfo responseInfo) {
        int contentLength = Integer.parseInt(
                responseInfo.headers().firstValue("Content-Length").orElse("0"));
        if (contentLength == 0) {
            try {
                return HttpResponse.BodySubscribers.replacing(DocumentBuilderFactory.newInstance()
                        .newDocumentBuilder()
                        .newDocument());
            } catch (ParserConfigurationException e) {
                throw new UncheckedIOException(new IOException("Failed to parse XML", e));
            }
        } else {
            return HttpResponse.BodySubscribers.mapping(
                    HttpResponse.BodySubscribers.ofInputStream(), XmlBodyHandler::parseXml);
        }
    }

    /**
     * Parses the given InputStream as XML and returns a Document.
     *
     * @param inputStream the InputStream to parse
     * @return parsed XML Document
     * @throws UncheckedIOException if an error occurs during parsing
     */
    private static Document parseXml(InputStream inputStream) {
        try {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            factory.setNamespaceAware(true);
            DocumentBuilder builder = factory.newDocumentBuilder();
            return builder.parse(inputStream);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to parse XML", e);
        } catch (ParserConfigurationException | SAXException e) {
            throw new UncheckedIOException(new IOException("Failed to parse XML", e));
        }
    }
}
