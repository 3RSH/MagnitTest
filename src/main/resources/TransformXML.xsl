<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
  <xsl:output method="xml" indent="yes"/>

  <xsl:template match="/">

    <entries>

      <xsl:for-each select="entries/entry">
        <xsl:variable name="var">
          <xsl:value-of select="field"/>
        </xsl:variable>

        <entry field="{field}"/>
      </xsl:for-each>

    </entries>

</xsl:template>
</xsl:stylesheet>


