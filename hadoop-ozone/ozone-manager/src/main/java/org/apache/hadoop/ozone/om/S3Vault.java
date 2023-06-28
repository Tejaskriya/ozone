package org.apache.hadoop.ozone.om;

    import org.apache.hadoop.ozone.om.helpers.S3SecretValue;

    import java.io.IOException;

    import java.util.HashMap;
    import java.util.Map;

    import com.bettercloud.vault.Vault;
    import com.bettercloud.vault.VaultConfig;
    import com.bettercloud.vault.VaultException;
    import com.bettercloud.vault.response.LogicalResponse;

public class S3Vault implements S3SecretStore, S3SecretCache {
    private String host;
    private String token;
    private String unsealKey;
    private Vault vault;

    public S3Vault(){
        host = new String("<http://10.200.5.82:8210>");
        unsealKey=new String("ItyvMKlx44OiGsN7w+T2cJcUV5saDkQFFSWUJy7/Qcc=");
        token=new String("hvs.J6X0yS8QBsNckBwEp0V5UUd7");
        try {
            final VaultConfig config =
                new VaultConfig()
                    .address(host)
                    .token(token)
                    .build();
            vault = new Vault(config, 1);
        } catch (VaultException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void storeSecret(String kerberosId, S3SecretValue secret)
        throws IOException{
        final Map<String, Object> secrets = new HashMap<String, Object>();
        secrets.put(kerberosId,secret);
        String secretsPath="kv/"+kerberosId;
        try {
            final LogicalResponse writeResponse = vault.logical()
                .write(secretsPath, secrets);
            if (writeResponse.getRestResponse().getStatus()/100!=2){
                throw new IOException();
            }
        }
        catch (VaultException e){

        }
    }
    @Override
    public S3SecretValue getSecret(String kerberosID) throws IOException{
        S3SecretValue s3SecretValue=null;
        try {
    /*final String value = vault.logical()
            .read("kv/"+kerberosID)
            .getData().get(kerberosID);
       String[] valueArray=value.split("\\\\n",2);
        String[] temp = new String[2];
        for(int i=0;i<2;i++) {
            temp = valueArray[i].split("=", 0);
            String s = temp[1];
            valueArray[i] = s;
        }*/
            final Map<String,String> value = vault.logical()
                .read("kv/"+kerberosID)
                .getData();
            if (value.isEmpty())
                return null;
            String[] s= value.values().toArray(new String[2]);
            String[] a=s[0].split("\\\\n",0);
            s[0]=a[0].split("=",0)[1];
            s[1]=a[1].split("=",0)[1];

            s3SecretValue=new S3SecretValue(s[0],s[1]);

            //s3SecretValue=new S3SecretValue(valueArray[0],valueArray[1]);
        }
        catch (VaultException e){

        }
        return s3SecretValue;
    }
    @Override
    public void revokeSecret(String kerberosId) throws IOException{
        try {
            final LogicalResponse deleteResponse = vault.logical()
                .delete("kv/"+kerberosId);
            if(deleteResponse.getRestResponse().getStatus()/100!=2){
                throw new IOException();
            }
        }
        catch (VaultException e){

        }
    }
    @Override
    public S3Batcher batcher(){
        return null;
    }

    /*@Override
    public void put(String id, S3SecretValue secretValue, long txId){
        try {
            this.storeSecret(id,secretValue);
        } catch (IOException e) {
            //throw new RuntimeException(e);
        }
    }*/

    @Override
    public void invalidate(String id){

    }
    @Override
    public void put(String id, S3SecretValue secretValue) {
        try {
            this.storeSecret(id,secretValue);
        } catch (IOException e) {
            //throw new RuntimeException(e);
        }
    }


    @Override
    public S3SecretValue get(String id) {
        //return cache.getIfPresent(id);
        //this.getSecret(id);
        //return null;
        S3SecretValue s3SecretValue=null;
        try {
            s3SecretValue=this.getSecret(id);
        } catch (IOException e) {
            //throw new RuntimeException(e);
        }
        return s3SecretValue;
    }

}
