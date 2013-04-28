<?php
/**
 * DynamoDB orm class file.
 *
 * @author Yijie Li
 * @link http://www.userreport.com/
 * @copyright userreport
 * @since 2.0
 */

namespace main\orm;

use Exception;
use AmazonDynamoDB;

abstract class DynamoORM
{
    const HASHKEY  = 'hashKey';
    const RANGEKEY = 'rangeKey';
    
    /**
     * @var dynamoDB a dynamoDB object
     */
    private static $dynamoDB;
    /**
     * @var array the object represented in an array 
     */
    protected $attributes = array();
    /**
     * @var array the orignal value when the object is created
     */
    protected $oldAttributes = array();
    
    /**
     * constructor
     */
    public function __construct(){
    }
    
    /**
     * get the name of table
     * @return string the name of table
     */
    abstract protected function tableName();
    
    /**
     * specify the rules for each attribute
     * this must contain 'required' rule which contians the hash key and rang key property names
     * @return string the name of table
     */
    abstract protected function rules();

    /**
     * validate the value of data specified in the rules 
     * @return boolean true if pass, false otherwise
     */
    abstract function validateAttributes();
    
    /**
     * create a dynamoDb client, used to communicate with dynamoDB
     * @return DynamoDbClient the dynamodb client object
     */
    protected static function getDynamoDB()
    {
        if(self::$dynamoDB == null)
        {
            self::$dynamoDB = new AmazonDynamoDB();
            self::$dynamoDB->parse_the_response = false;
        }
        return self::$dynamoDB;
    }
    
    /**
     * get the primary keys of the table
     * @return array the primary key contains hash key and/or range key 
     */
    public static function getPkKeys()
    {
        $rules = static::model()->rules();
        return $rules['required'];
    }
    
    
    /**
     * @return mix return a static instance of the class, used for invoking static methods.
     */
    public static function model(){
        return new static();
    }
    
    /**
     * get the property type correponding to the column type in table
     * @return string type name
     */
    public static function getType($property)
    {
        return static::model()->rules()['type'][$property];
    }
    
    /**
     * find one item based on the given primary key
     * @param mix $hkey the hash key 
     * @param mix $rkey the range key
     * @return mix the object if found, null otherwise
     */
    public static function findOne($hkey, $rkey = null){
        $dynamodb = self::getDynamoDB();
        
        $response = $dynamodb->get_item( array(
            'TableName' => static::model()->tableName(),
            'Key'       => $dynamodb->attributes( array(
                'HashKeyElement'  => $hkey,
                'RangeKeyElement' => $rkey,
            )),
        ));
        
        if(!$response->isOK())
            throw new Exception(json_decode($response->body, TRUE)['message']);
        
        $response = json_decode($response->body, TRUE);
        // Success?
        if (empty($response['Item']))
            return null;
        return static::arrayToObject($response['Item']);
    }
    
    /**
     * find all items based on the given hashKey key
     * @param mix $hkey the hash key
     * @return array  an array of the objects
     */
    public static function findAllByHk($hkey){
        
        $key = static::getPkKeys();

        $query = array(
            'TableName'     => static::model()->tableName(),
            'HashKeyValue'  => array(
                static::getType($key[self::HASHKEY]) => $hkey,
            )
        );
            
        return static::sendQuery('query', $query);   
    }
    
    /**
     * find by any attribute name
     * @param string $key the key name
     * @param string $value the value
     * @param string $comparison the comparison
     * @return array an array of found objects
     */
    protected static function findAllBy($key, $value, $comparison)
    {
        $query = array(
            'TableName'     => static::model()->tableName(),
            'ScanFilter'    => array(
                $key    => array(
                    'ComparisonOperator' => $comparison,
                    'AttributeValueList' => array(
                        array(static::getType($key) => $value),
                    )
                ),
            ),
        );

        return static::sendQuery('scan', $query);   
    }
    
    /**
     * find all models in the table
     * @return array an array of all objects
     */
    public static function findAll()
    {
        $query = array(
            'TableName'     => static::model()->tableName(),
            'ScanFilter'    => array(
                static::getPkKeys()[self::HASHKEY] => array(
                    'ComparisonOperator' => 'NOT_NULL',
                )
            ),
        );

        return static::sendQuery('scan', $query); 
    }
    
    /**
     *  a helper method to send query and parse results
     *  @param array $query query array
     *  @return array an array of objects, empty array if there is no results.
     */
    protected static function sendQuery($method, $query)
    {
        $dynamodb = self::getDynamoDB();
        
        $rawItems = array();
        $response = array();
        
        do {
            $response = $dynamodb->$method($query);
            if($response->isOk())
            {
                $response = json_decode($response->body, TRUE);
                //add the pager for next query
                $query['ExclusiveStartKey'] = empty($response['LastEvaluatedKey']) ? null : $response['LastEvaluatedKey'];
    
                // Success?
                if($response['Count'] == 1)
                    $rawItems[] = $response['Items'];
                else
                    $rawItems = array_merge($rawItems, $response['Items']);
            }
            else
                throw new Exception(json_decode($response->body, TRUE)['message']);
        }
        while (isset($response) && !empty($response['LastEvaluatedKey']));

        if (empty($rawItems) )
            return $rawItems;
        
        $objs = array();
        //convert to objects
        foreach ($rawItems as $item)
            $objs[] = static::arrayToObject($item);

        return $objs;
    }
    
    
    /**
     * a helper method to conver array to the current object
     */
    protected static function arrayToObject($arr)
    {
        $obj = new static();
        foreach($arr as $key => $value)
        {
            $v = reset($value);
            $k = key($value);
            switch($k)
            {
                case 'B' :
                case 'BS':
                case 'S' :
                case 'SS': $obj->$key = $obj->oldAttributes[$key] = $v; break;
                case 'NS': $obj->$key = $obj->oldAttributes[$key] = array_walk($v, 'intval'); break;
                case 'N':  $obj->$key = $obj->oldAttributes[$key] = intval($v);
            }
        }
        return $obj; 
    }
    
    /**
     * save the item represented by this new object
     * @return boolean true if success, false otherwise
     */
    public function save()
    {
        $dynamodb = self::getDynamoDB();
        $response = $dynamodb->put_item(array(
            'TableName' => static::model()->tableName(),
            'Item'      => $dynamodb->attributes(get_object_vars($this)),
        ));
        if ($response->isOk())
        {
            $this->oldAttributes = get_object_vars($this);
            return true;
        }
        else
            throw new Exception(json_decode($response->body, TRUE)['message']);
    }
    
    
    /**
     * updates the item represented by this object
     * @param mix $data the array of key-value paired attributes, all attributes having values must be presented
     * @return boolean true if success, false otherwise
     */
    public function update()
    {
        $keys    = static::getPkKeys();
        $allKeys = array_keys($this->rules()['type']);
        $pkKeys  = array_values($keys);
        $pUpdate = array_diff($allKeys, $pkKeys);
        
        $oldAttrs = $this->oldAttributes;
        //compare and see what need to change
        $actions = array();
        
        foreach($pUpdate as $pro)
        {
            if (empty($this->$pro) && !empty($oldAttrs[$pro]))
            {
                $actions[$pro] = array(
                    'Action' => AmazonDynamoDB::ACTION_DELETE
                );
            }
            else if (!empty($this->$pro) && empty($oldAttrs[$pro]) && is_numeric($this->$pro))
            {
                $actions[$pro] = array(
                    'Action' => AmazonDynamoDB::ACTION_ADD,
                    'Value'  => array( static::getType($pro) => strval($this->$pro)),
                );
            }
            else if (!empty($this->$pro) || !empty($oldAttrs[$pro]))
            {
                $actions[$pro] = array(
                    'Action' => AmazonDynamoDB::ACTION_PUT,
                    'Value'  => array( static::getType($pro) => strval($this->$pro)),
                );
            }
        }
       
        //lets send the update query
        $dynamodb = self::getDynamoDB();
        $response = $dynamodb->update_item( array(
            'TableName' => static::model()->tableName(),
            'Key'       => $dynamodb->attributes( array(
                'HashKeyElement'  => $this->{$keys[self::HASHKEY]},
                'RangeKeyElement' => isset($keys[self::RANGEKEY])? $this->{$keys[self::RANGEKEY]} : null,
            )),
            'AttributeUpdates' => $actions,
        ));
        
        if ($response->isOk())
        {
            $this->oldAttributes = get_object_vars($this);
            return true;
        }
        else
            throw new Exception(json_decode($response->body, TRUE)['message']);
    }
    
    /**
     * delete the item corresponding to the current object
     * @return boolean true if success, false otherwise
     */
    public function delete()
    {
        $keys = static::getPkKeys();
        $dynamodb = self::getDynamoDB();

        $response  = $dynamodb->delete_item( array(
            'TableName' => static::model()->tableName(),
            'Key'       => $dynamodb->attributes(array(
                'HashKeyElement'  => $this->{$keys[self::HASHKEY]},
                'RangeKeyElement' => isset($keys[self::RANGEKEY])? $this->{$keys[self::RANGEKEY]} : null,
            )),
        ));
        
        if($response->isOK())
            return true;
        else
            throw new Exception(json_decode($response->body, TRUE)['message']);
    }
    
    /**
     * get attributes method
     */
    public function getAttributes()
    {
        return get_object_vars($this);
    }
    
    /**
     * set attributes method
     * @param mix $value the value of the property
     */
    public function setAttributes(array $data)
    {
        foreach($data as $k => $v)
        {
            if(property_exists($this, $k))
                $this->$k = $v;
        }
        return $this;
    }
    
    /**
     * magic getter method, do not call this method directly
     */
    public function __get($name)
    {
        $getter='get'.$name;
        if (method_exists($this,$getter))
            return $this->$getter();
    }

    /**
     * magic setter method, do not call this method directly
     * @param mix $value the value of the property
     */
    public function __set($name,$value)
    {
        $setter='set'.$name;
        if (method_exists($this,$setter))
            return $this->$setter($value);
    }
    
    /**
     * magic call method, do not call this method directly
     * @param mix $name the name of method
     * @param mix $arguments the arguments
     */
    public static function __callStatic($name, $params)
    {
        $needle = 'findAllBy';
        if (!strncmp($name, $needle, strlen($needle)))
        {
            $name= strtolower(str_replace($needle, '', $name));
            return static::findAllBy($name, $params[0], $params[1]);
        }
    }
}

//end of class